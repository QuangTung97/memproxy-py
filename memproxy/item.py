from __future__ import annotations

import dataclasses
import json
import logging
from dataclasses import dataclass
from typing import Generic, TypeVar, Callable, Any, List, Optional, Dict

from .memproxy import LeaseGetResult
from .memproxy import Promise, Pipeline, Session, LeaseGetStatus

T = TypeVar("T")
K = TypeVar("K")

KeyNameFunc = Callable[[K], str]  # K -> str
FillerFunc = Callable[[K], Promise[T]]  # K -> Promise[T]


@dataclass
class ItemCodec(Generic[T]):
    encode: Callable[[T], bytes]
    decode: Callable[[bytes], T]


class DataclassJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)


def new_json_codec(cls: Any) -> ItemCodec[T]:
    return ItemCodec(
        encode=lambda x: json.dumps(x, cls=DataclassJSONEncoder).encode(),
        decode=lambda d: cls(**json.loads(d)),
    )


class _ItemConfig(Generic[T, K]):
    pipe: Pipeline
    key_fn: KeyNameFunc
    sess: Session
    codec: ItemCodec
    filler: FillerFunc

    hit_count: int
    fill_count: int
    cache_error_count: int
    decode_error_count: int

    def __init__(
            self, pipe: Pipeline,
            key_fn: Callable[[K], str], filler: Callable[[K], Promise[T]],
            codec: ItemCodec[T],
    ):
        self.pipe = pipe
        self.key_fn = key_fn
        self.sess = pipe.lower_session()
        self.filler = filler
        self.codec = codec

        self.hit_count = 0
        self.fill_count = 0
        self.cache_error_count = 0
        self.decode_error_count = 0


class _ItemState(Generic[T, K]):
    _conf: _ItemConfig[T, K]

    key: K
    key_str: str
    lease_get_fn: LeaseGetResult
    cas: int

    result: T

    def __init__(
            self,
            conf: _ItemConfig[T, K],
            key: K,
    ):
        self._conf = conf
        self.key = key
        self.key_str = self._conf.key_fn(key)
        self.lease_get_fn = self._conf.pipe.lease_get(self.key_str)

    def _handle_set_back(self):
        data = self._conf.codec.encode(self.result)
        set_fn = self._conf.pipe.lease_set(key=self.key_str, cas=self.cas, data=data)

        def handle_set_fn():
            set_fn()

        self._conf.sess.add_next_call(handle_set_fn)

    def _handle_fill_fn(self):
        self.result = self._fill_fn()

        if self.cas <= 0:
            return

        self._conf.sess.add_next_call(self._handle_set_back)

    def _handle_filling(self):
        self._conf.fill_count += 1
        self._fill_fn = self._conf.filler(self.key)
        self._conf.sess.add_next_call(self._handle_fill_fn)

    def __call__(self) -> None:
        get_resp = self.lease_get_fn.result()

        if get_resp.status == LeaseGetStatus.FOUND:
            self._conf.hit_count += 1
            try:
                self.result = self._conf.codec.decode(get_resp.data)
                return
            except Exception as e:
                self._conf.decode_error_count += 1
                get_resp.error = f'Decode error. {str(e)}'

        if get_resp.status == LeaseGetStatus.LEASE_GRANTED:
            self.cas = get_resp.cas
        else:
            if get_resp.status == LeaseGetStatus.ERROR:
                self._conf.cache_error_count += 1
            logging.error('Item get error. %s', get_resp.error)
            self.cas = 0

        self._handle_filling()

    def result_func(self) -> T:
        self._conf.sess.execute()
        r = self.result
        release_item_state(self)
        return r


item_state_pool: List[Any] = []


def new_item_state(conf: _ItemConfig[T, K], key: K) -> _ItemState[T, K]:
    if len(item_state_pool) == 0:
        return _ItemState(conf, key)
    e = item_state_pool.pop()
    e.__init__(conf, key)
    return e


def release_item_state(state: _ItemState[T, K]):
    if len(item_state_pool) >= 4096:
        return
    item_state_pool.append(state)


class Item(Generic[T, K]):
    _conf: _ItemConfig[T, K]

    def __init__(
            self, pipe: Pipeline,
            key_fn: Callable[[K], str], filler: Callable[[K], Promise[T]],
            codec: ItemCodec[T],
    ):
        self._conf = _ItemConfig(pipe=pipe, key_fn=key_fn, filler=filler, codec=codec)

    def _get_fast(self, key: K) -> _ItemState[T, K]:
        state = new_item_state(self._conf, key)
        self._conf.sess.add_next_call(state)
        return state

    def get(self, key: K) -> Promise[T]:
        state = self._get_fast(key)
        return state.result_func

    def get_multi(self, keys: List[K]) -> Promise[List[T]]:
        states: List[_ItemState[T, K]] = []

        for k in keys:
            st = self._get_fast(k)
            states.append(st)

        def result_func() -> List[T]:
            result: List[T] = []
            for state in states:
                result.append(state.result_func())
            return result

        return result_func

    def compute_key_name(self, key: K) -> str:
        return self._conf.key_fn(key)

    @property
    def hit_count(self) -> int:
        return self._conf.hit_count

    @property
    def fill_count(self) -> int:
        return self._conf.fill_count

    @property
    def cache_error_count(self) -> int:
        return self._conf.cache_error_count

    @property
    def decode_error_count(self) -> int:
        return self._conf.decode_error_count


class _MultiGetState(Generic[T, K]):
    keys: List[K]
    result: Dict[K, T]
    completed: bool = False

    def __init__(self):
        self.keys = []
        self.completed = False
        self.result = {}

    def add_key(self, key: K):
        self.keys.append(key)


MultiGetFillFunc = Callable[[List[K]], List[T]]  # [K] -> [T]
GetKeyFunc = Callable[[T], K]  # T -> K


class _MultiGetFunc(Generic[T, K]):
    _state: Optional[_MultiGetState]
    _fill_func: MultiGetFillFunc
    _get_key_func: GetKeyFunc
    _default: T

    def __init__(self, fill_func: MultiGetFillFunc, key_func: GetKeyFunc, default: T):
        self._state = None
        self._fill_func = fill_func
        self._get_key_func = key_func
        self._default = default

    def _get_state(self) -> _MultiGetState:
        if self._state is None:
            self._state = _MultiGetState()
        return self._state

    def result_func(self, key: K) -> Promise[T]:
        state = self._get_state()
        state.add_key(key)

        def resp_func() -> T:
            if not state.completed:
                values = self._fill_func(state.keys)

                for v in values:
                    k = self._get_key_func(v)
                    state.result[k] = v

                state.completed = True
                self._state = None

            return state.result.get(key, self._default)

        return resp_func


# from [K] -> [T] to K -> Promise[T]
def new_multi_get_filler(
        fill_func: MultiGetFillFunc[K, T],
        get_key_func: GetKeyFunc,
        default: T,
) -> FillerFunc:
    fn = _MultiGetFunc[T, K](fill_func=fill_func, key_func=get_key_func, default=default)
    return fn.result_func
