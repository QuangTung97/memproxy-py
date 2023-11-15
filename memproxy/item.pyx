import dataclasses
import json
import logging

from memproxy.session cimport Session

cdef class ItemCodec:
    cdef:
        public object encode
        public object decode

    def __init__(self, object encode, object decode):
        self.encode = encode
        self.decode = decode


class DataclassJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)


def new_json_codec(object cls):
    return ItemCodec(
        encode=lambda x: json.dumps(x, cls=DataclassJSONEncoder).encode(),
        decode=lambda d: cls(**json.loads(d)),
    )


cdef class _ItemConfig:
    cdef:
        object pipe
        object key_fn
        Session sess
        ItemCodec codec
        object filler

        size_t hit_count
        size_t fill_count
        size_t cache_error_count
        size_t decode_error_count

    def __init__(
            self, object pipe,
            object key_fn,
            object filler,
            ItemCodec codec,
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


cdef class _ItemState:
    cdef:
        _ItemConfig conf

        object key
        str key_str
        object lease_get_fn
        size_t cas

        object _fill_fn

        object result

    cdef void _handle_set_back(self):
        data = self.conf.codec.encode(self.result)
        set_fn = self.conf.pipe.lease_set(key=self.key_str, cas=self.cas, data=data)

        def handle_set_fn():
            set_fn()

        self.conf.sess.add_next_call(handle_set_fn)

    cdef void _handle_fill_fn(self):
        self.result = self._fill_fn()

        if self.cas <= 0:
            return

        self.conf.sess.add_next_call(self._handle_set_back)

    cdef void _handle_filling(self):
        self.conf.fill_count += 1
        self._fill_fn = self.conf.filler(self.key)
        self.conf.sess.add_next_call(self._handle_fill_fn)

    def __call__(self) -> None:
        cdef object get_resp = self.lease_get_fn.result()

        cdef str resp_error = get_resp[3]
        if get_resp[0] == 1:
            self.conf.hit_count += 1
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

    def result_func(self):
        if self.conf.sess.is_dirty:
            self.conf.sess.execute()

        r = self.result

        if len(item_state_pool) < 4096:
            item_state_pool.append(self)

        return r


cdef list item_state_pool = []
cdef list _P = item_state_pool


cdef class Item:
    cdef:
        _ItemConfig _conf

    def __init__(
            self, object pipe,
            object key_fn, object filler,
            ItemCodec codec,
    ):
        self._conf = _ItemConfig(pipe=pipe, key_fn=key_fn, filler=filler, codec=codec)

    def get(self, object key):
        # do init item state
        cdef _ItemState state
        if len(_P) == 0:
            state = _ItemState()
        else:
            state = _P.pop()

        state.conf = self._conf
        state.key = key
        state.key_str = self._conf.key_fn(key)
        state.lease_get_fn = self._conf.pipe.lease_get(state.key_str)
        # end init item state

        self._conf.sess.add_next_call(state)

        return state.result_func

    def get_multi(self, list keys):
        cdef list states = []
        cdef _ItemState state
        cdef object key

        for key in keys:
            # do init item state
            if len(_P) == 0:
                state = _ItemState()
            else:
                state = _P.pop()

            state.conf = self._conf
            state.key = key
            state.key_str = self._conf.key_fn(key)
            state.lease_get_fn = self._conf.pipe.lease_get(state.key_str)
            # end init item state

            self._conf.sess.add_next_call(state)

            states.append(state)

        def result_func():
            cdef list result = []
            for resp_state in states:
                result.append(resp_state.result_func())
            return result

        return result_func

    def compute_key_name(self, object key) -> str:
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


cdef class _MultiGetState:
    cdef:
        list keys
        dict result
        bint completed

    def __init__(self):
        self.keys = []
        self.completed = False
        self.result = {}

    cdef add_key(self, object key):
        self.keys.append(key)


cdef class _MultiGetFunc:
    cdef:
        _MultiGetState _state
        object _fill_func
        object _get_key_func
        object _default

    def __init__(self, object fill_func, object key_func, object default):
        self._state = None
        self._fill_func = fill_func
        self._get_key_func = key_func
        self._default = default

    cdef _MultiGetState _get_state(self):
        if self._state is None:
            self._state = _MultiGetState()
        return self._state

    def result_func(self, object key):
        cdef _MultiGetState state = self._get_state()
        state.add_key(key)

        def resp_func():
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
        object fill_func,
        object get_key_func,
        object default,
):
    fn = _MultiGetFunc(fill_func=fill_func, key_func=get_key_func, default=default)
    return fn.result_func
