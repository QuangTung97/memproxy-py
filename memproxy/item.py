from __future__ import annotations

import dataclasses
import json
import logging
from dataclasses import dataclass
from typing import Generic, TypeVar, Callable, Any, List, Optional, Dict, Type

from .memproxy import LeaseGetResult
from .memproxy import Promise, Pipeline, Session

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


def new_json_codec(cls: Type[T]) -> ItemCodec[T]:
    return ItemCodec(
        encode=lambda x: json.dumps(x, cls=DataclassJSONEncoder).encode(),
        decode=lambda d: cls(**json.loads(d)),
    )


class _ItemConfig(Generic[T, K]):
    __slots__ = (
        'pipe', 'key_fn', 'sess', 'codec', 'filler',
        'hit_count', 'fill_count', 'cache_error_count', 'decode_error_count',
        'bytes_read'
    )

    pipe: Pipeline
    key_fn: KeyNameFunc
    sess: Session
    codec: ItemCodec
    filler: FillerFunc

    hit_count: int
    fill_count: int
    cache_error_count: int
    decode_error_count: int
    bytes_read: int

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
        self.bytes_read = 0


class _ItemState(Generic[T, K]):
    __slots__ = (
        'conf', 'key', 'key_str', 'lease_get_fn', 'cas', '_fill_fn', 'result',
    )

    conf: _ItemConfig[T, K]

    key: K
    key_str: str
    lease_get_fn: LeaseGetResult
    cas: int

    _fill_fn: Promise[T]

    result: T

    def _handle_set_back(self):
        data = self.conf.codec.encode(self.result)
        set_fn = self.conf.pipe.lease_set(key=self.key_str, cas=self.cas, data=data)

        def handle_set_fn():
            set_fn()

        self.conf.sess.add_next_call(handle_set_fn)

    def _handle_fill_fn(self):
        self.result = self._fill_fn()

        if self.cas <= 0:
            return

        self.conf.sess.add_next_call(self._handle_set_back)

    def _handle_filling(self):
        self.conf.fill_count += 1
        self._fill_fn = self.conf.filler(self.key)
        self.conf.sess.add_next_call(self._handle_fill_fn)

    def __call__(self) -> None:
        get_resp = self.lease_get_fn.result()

        resp_error: Optional[str] = get_resp[3]
        if get_resp[0] == 1:
            self.conf.hit_count += 1
            self.conf.bytes_read += len(get_resp[1])
            try:
                self.result = self.conf.codec.decode(get_resp[1])
                return
            except Exception as e:
                self.conf.decode_error_count += 1
                resp_error = f'Decode error. {str(e)}'

        if get_resp[0] == 2:
            self.cas = get_resp[2]
        else:
            if get_resp[0] == 3:
                self.conf.cache_error_count += 1
            logging.error('Item get error. %s', resp_error)
            self.cas = 0

        self._handle_filling()

    def result_func(self) -> T:
        if self.conf.sess.is_dirty:
            self.conf.sess.execute()

        r = self.result
        return r


class Item(Generic[T, K]):
    __slots__ = '_conf'

    _conf: _ItemConfig[T, K]

    def __init__(
            self, pipe: Pipeline,
            key_fn: Callable[[K], str],  # K -> str
            filler: Callable[[K], Promise[T]],  # K -> () -> T
            codec: ItemCodec[T],
    ):
        self._conf = _ItemConfig(pipe=pipe, key_fn=key_fn, filler=filler, codec=codec)

    def get(self, key: K) -> Promise[T]:
        # do init item state
        state: _ItemState[T, K] = _ItemState()

        state.conf = self._conf
        state.key = key
        state.key_str = self._conf.key_fn(key)
        state.lease_get_fn = self._conf.pipe.lease_get(state.key_str)
        # end init item state

        self._conf.sess.add_next_call(state)

        return state.result_func

    def get_multi(self, keys: List[K]) -> Callable[[], List[T]]:
        conf = self._conf
        key_fn = conf.key_fn
        pipe = conf.pipe
        sess: Session = self._conf.sess

        states: List[_ItemState[T, K]] = []

        for key in keys:
            # do init item state
            state: _ItemState[T, K] = _ItemState()

            state.conf = conf
            state.key = key
            state.key_str = key_fn(key)
            state.lease_get_fn = pipe.lease_get(state.key_str)
            # end init item state

            sess.add_next_call(state)

            states.append(state)

        def result_func() -> List[T]:
            result: List[T] = []
            for resp_state in states:
                result.append(resp_state.result_func())
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

    @property
    def bytes_read(self) -> int:
        return self._conf.bytes_read


class _MultiGetState(Generic[T, K]):
    __slots__ = ('keys', 'result', 'completed')

    keys: List[K]
    result: Dict[K, T]
    completed: bool

    def __init__(self):
        self.keys = []
        self.completed = False
        self.result = {}

    def add_key(self, key: K):
        self.keys.append(key)


MultiGetFillFunc = Callable[[List[K]], List[T]]  # [K] -> [T]
GetKeyFunc = Callable[[T], K]  # T -> K


class _MultiGetFunc(Generic[T, K]):
    __slots__ = '_state', '_fill_func', '_get_key_func', '_default'

    _state: Optional[_MultiGetState[T, K]]
    _fill_func: MultiGetFillFunc
    _get_key_func: GetKeyFunc
    _default: T

    def __init__(
            self,
            fill_func: Callable[[List[K]], List[T]],  # List[K] -> List[T]
            key_func: Callable[[T], K],  # T -> K
            default: T,
    ):
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
        fill_func: Callable[[List[K]], List[T]],  # List[K] -> List[T]
        get_key_func: Callable[[T], K],  # T -> K
        default: T,
) -> Callable[[K], Promise[T]]:  # K -> () -> T
    fn = _MultiGetFunc(fill_func=fill_func, key_func=get_key_func, default=default)
    return fn.result_func
