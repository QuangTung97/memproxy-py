import dataclasses
import json
import logging
from dataclasses import dataclass
from typing import Generic, TypeVar, Callable, Any, List, Optional, Dict

from .memproxy import Promise, Pipeline, Session, LeaseGetResponse, LeaseGetStatus

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


class _ItemState(Generic[T, K]):
    _pipe: Pipeline
    sess: Session
    _codec: ItemCodec
    _filler: FillerFunc

    key: K
    key_str: str
    lease_get_fn: Promise[LeaseGetResponse]
    cas: int

    result: T

    def __init__(
            self, pipe: Pipeline, sess: Session,
            codec: ItemCodec,
            filler: FillerFunc,
    ):
        self._pipe = pipe
        self.sess = sess
        self._codec = codec
        self._filler = filler

    def _handle_set_back(self):
        data = self._codec.encode(self.result)

        set_fn = self._pipe.lease_set(key=self.key_str, cas=self.cas, data=data)

        def handle_set_fn():
            set_fn()

        self.sess.add_next_call(handle_set_fn)

    def _handle_fill_fn(self):
        self.result = self._fill_fn()

        if self.cas <= 0:
            return

        self.sess.add_next_call(self._handle_set_back)

    def _handle_filling(self):
        self._fill_fn = self._filler(self.key)
        self.sess.add_next_call(self._handle_fill_fn)

    def next_fn(self) -> None:
        get_resp = self.lease_get_fn()

        if get_resp.status == LeaseGetStatus.FOUND:
            try:
                self.result = self._codec.decode(get_resp.data)
                return
            except Exception as e:
                get_resp.error = f'Decode error. {str(e)}'

        if get_resp.status == LeaseGetStatus.LEASE_GRANTED:
            self.cas = get_resp.cas
        else:
            logging.error('Item get error. %s', get_resp.error)
            self.cas = 0

        self._handle_filling()


class Item(Generic[T, K]):
    _pipe: Pipeline
    _sess: Session
    _key_fn: KeyNameFunc[K]
    _codec: ItemCodec[T]
    _filler: FillerFunc[K, T]

    def __init__(
            self, pipe: Pipeline,
            key_fn: Callable[[K], str], filler: Callable[[K], Promise[T]],
            codec: ItemCodec[T],
    ):
        self._pipe = pipe
        self._sess = pipe.lower_session()
        self._key_fn = key_fn
        self._filler = filler
        self._codec = codec

    def get(self, key: K) -> Promise[T]:
        state = _ItemState[T, K](
            pipe=self._pipe,
            sess=self._sess,
            codec=self._codec,
            filler=self._filler,
        )

        state.key = key
        state.key_str = self._key_fn(key)
        state.lease_get_fn = self._pipe.lease_get(state.key_str)

        self._sess.add_next_call(state.next_fn)

        def get_fn() -> T:
            state.sess.execute()
            return state.result

        return get_fn

    def get_multi(self, keys: List[K]) -> Promise[List[T]]:
        fn_list: List[Promise[T]] = []

        for k in keys:
            fn = self.get(k)
            fn_list.append(fn)

        def result_func() -> List[T]:
            result: List[T] = []
            for item_fn in fn_list:
                result.append(item_fn())
            return result

        return result_func

    def compute_key_name(self, key: K) -> str:
        return self._key_fn(key)


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
