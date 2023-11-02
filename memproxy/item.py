from typing import Generic, TypeVar, Callable

from .memproxy import Promise, Pipeline, Session, LeaseGetResponse, LeaseGetStatus

T = TypeVar("T")
K = TypeVar("K")

KeyNameFunc = Callable[[K], str]  # K -> str
FillerFunc = Callable[[K], Promise[T]]  # K -> Promise[T]
EncoderFunc = Callable[[T], bytes]  # T -> bytes
DecoderFunc = Callable[[bytes], T]  # bytes -> T


class _ItemState(Generic[T, K]):
    _decoder: DecoderFunc
    sess: Session

    key_str: str
    lease_get_fn: Promise[LeaseGetResponse]
    result: T

    def __init__(self, sess: Session, decoder: DecoderFunc):
        self.sess = sess
        self._decoder = decoder

    def next_fn(self) -> None:
        get_resp = self.lease_get_fn()

        if get_resp.status == LeaseGetStatus.FOUND:
            self.result = self._decoder(get_resp.data)
            return


class Item(Generic[T, K]):
    _pipe: Pipeline
    _sess: Session
    _key_fn: KeyNameFunc
    _filler: FillerFunc
    _encoder: EncoderFunc
    _decoder: DecoderFunc

    def __init__(
            self, pipe: Pipeline,
            key_fn: KeyNameFunc, filler: FillerFunc,
            encoder=EncoderFunc,
            decoder=DecoderFunc,
    ):
        self._pipe = pipe
        self._sess = pipe.lower_session()
        self._key_fn = key_fn
        self._filler = filler
        self._encoder = encoder
        self._decoder = decoder

    def get(self, key: K) -> Promise[T]:
        state = _ItemState[T, K](sess=self._sess, decoder=self._decoder)

        state.key_str = self._key_fn(key)
        state.lease_get_fn = self._pipe.lease_get(state.key_str)

        self._sess.add_next_call(state.next_fn)

        def get_fn() -> T:
            state.sess.execute()
            return state.result

        return get_fn

    def compute_key_name(self, key: K) -> str:
        return self._key_fn(key)


def new_json_item(
        pipe: Pipeline,
        key_fn: KeyNameFunc,
        filler: FillerFunc,
) -> Item[T, K]:
    return Item[T, K](
        pipe=pipe,
        key_fn=key_fn,
        filler=filler,
        encoder=lambda v: 10
    )
