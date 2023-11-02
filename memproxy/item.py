import dataclasses
import json
from dataclasses import dataclass
from typing import Generic, TypeVar, Callable, Any

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
        self.sess.add_next_call(self._handle_set_back)

    def _handle_filling(self):
        self._fill_fn = self._filler(self.key)
        self.sess.add_next_call(self._handle_fill_fn)

    def next_fn(self) -> None:
        get_resp = self.lease_get_fn()

        if get_resp.status == LeaseGetStatus.FOUND:
            # TODO Handle Exception
            self.result = self._codec.decode(get_resp.data)
            return

        if get_resp.status == LeaseGetStatus.LEASE_GRANTED:
            self.cas = get_resp.cas
            self._handle_filling()


class Item(Generic[T, K]):
    _pipe: Pipeline
    _sess: Session
    _key_fn: KeyNameFunc
    _codec: ItemCodec
    _filler: FillerFunc

    def __init__(
            self, pipe: Pipeline,
            key_fn: KeyNameFunc, filler: FillerFunc,
            codec: ItemCodec,
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

    def compute_key_name(self, key: K) -> str:
        return self._key_fn(key)
