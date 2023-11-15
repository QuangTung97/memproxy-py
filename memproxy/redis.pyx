from __future__ import annotations

import random

import redis
from redis.commands.core import Script

from .memproxy import LeaseGetResponse, LeaseSetResponse, DeleteResponse
from .memproxy import LeaseGetResult
from .memproxy import LeaseSetStatus, DeleteStatus
from .memproxy import Pipeline, Promise
from .session import Session

cdef str LEASE_GET_SCRIPT = """
local result = {}

for i = 1,#KEYS do
    local k = KEYS[i]
    
    local resp = redis.call('GET', k)
    
    if resp then
        result[i] = resp
    else
        local cas = redis.call('INCR', '__next_cas')
        local cas_str = 'cas:' .. cas
        redis.call('SET', k, cas_str, 'EX', 3)
        result[i] = cas_str
    end
end

return result
"""

cdef str LEASE_SET_SCRIPT = """
local result = {}

for i = 1,#KEYS do
    local k = KEYS[i]
    
    local resp = redis.call('GET', k)
    
    local cas_str = 'cas:' .. ARGV[i * 3 - 2]
    local val = 'val:' .. ARGV[i * 3 - 1]
    local ttl = ARGV[i * 3]
    
    if not resp then
        result[i] = 'NF'
    elseif resp ~= cas_str then
        result[i] = 'EX'
    else
        redis.call('SET', k, val, 'EX', ttl)
        result[i] = 'OK'
    end
end

return result
"""


cdef class SetInput:
    cdef:
        public str key
        public int cas
        public bytes val
        public int ttl

    def __init__(self, str key, int cas, bytes val, int ttl):
        self.key = key
        self.cas = cas
        self.val = val
        self.ttl = ttl


cdef class RedisPipelineState:
    cdef:
        RedisPipeline _pipe
        bint completed

        list keys
        list get_result

        list _set_inputs
        list set_result

        list _delete_keys
        list delete_result

        str redis_error

    def __init__(self, RedisPipeline pipe):
        self._pipe = pipe
        self.completed = False

        self.keys = []
        self._set_inputs = []
        self._delete_keys = []

        self.redis_error = None

    cdef int add_set_op(self, str key, int cas, bytes val, int ttl):
        cdef int index = len(self._set_inputs)
        self._set_inputs.append(SetInput(key=key, cas=cas, val=val, ttl=ttl))
        return index

    cdef int add_delete_op(self, str key):
        cdef int index = len(self._delete_keys)
        self._delete_keys.append(key)
        return index

    cdef void execute(self):
        try:
            self._execute_in_try()
        except Exception as e:
            self.redis_error = str(e)

    cdef void _execute_in_try(self):
        if len(self.keys) > 0:
            self._execute_lease_get()

        if len(self._set_inputs) > 0:
            self._execute_lease_set()

        if len(self._delete_keys):
            with self._pipe.client.pipeline(transaction=False) as pipe:
                for key in self._delete_keys:
                    pipe.delete(key)
                self.delete_result = pipe.execute()

        self.completed = True

    cdef void _execute_lease_get(self):
        batch_size = self._pipe.max_keys_per_batch

        if len(self.keys) <= batch_size:
            self.get_result = self._pipe.get_script(keys=self.keys, client=self._pipe.client)
            return

        with self._pipe.client.pipeline(transaction=False) as pipe:
            for n in range(0, len(self.keys), batch_size):
                keys = self.keys[n: n + batch_size]
                self._pipe.get_script(keys=keys, client=pipe)
            pipe_result = pipe.execute()

        self.get_result = []
        for r in pipe_result:
            self.get_result.extend(r)

    cdef void _execute_lease_set(self):
        batch_size = self._pipe.max_keys_per_batch

        if len(self._set_inputs) <= batch_size:
            self.set_result = self._execute_lease_set_inputs(self._set_inputs, self._pipe.client)
            return

        with self._pipe.client.pipeline(transaction=False) as pipe:
            for n in range(0, len(self._set_inputs), batch_size):
                inputs = self._set_inputs[n: n + batch_size]
                self._execute_lease_set_inputs(inputs, pipe)

            pipe_result = pipe.execute()

        self.set_result = []
        for r in pipe_result:
            self.set_result.extend(r)

    cdef object _execute_lease_set_inputs(self, list inputs, object client):
        cdef list keys = [i.key for i in inputs]
        cdef list args = []

        for i in inputs:
            args.append(i.cas)
            args.append(i.val)
            args.append(i.ttl)

        return self._pipe.set_script(keys=keys, args=args, client=client)


cdef class _RedisGetResult:
    cdef:
        RedisPipeline pipe
        RedisPipelineState state
        int index

    def result(self) -> LeaseGetResponse:
        if not self.state.completed:
            self.pipe.execute(self.state)

        if self.state.redis_error is not None:
            release_get_result(self)
            return 3, b'', 0, f'Redis Get: {self.state.redis_error}'

        get_resp = self.state.get_result[self.index]

        release_get_result(self)

        if get_resp.startswith(b'val:'):
            return 1, get_resp[len(b'val:'):], 0, None

        if get_resp.startswith(b'cas:'):
            num_str = get_resp[len(b'cas:'):].decode()
            if not num_str.isnumeric():
                return 3, b'', 0, f'Value "{num_str}" is not a number'

            cas = int(num_str)
            return 2, b'', cas, None
        else:
            return 1, get_resp, 0, None


cdef list get_result_pool = []
cdef list _P = get_result_pool


cdef void release_get_result(_RedisGetResult r):
    if len(_P) >= 4096:
        return
    _P.append(r)


cdef class RedisPipeline:
    cdef:
        object client
        object get_script
        object set_script
        object _sess
        int _min_ttl
        int _max_ttl
        int max_keys_per_batch
        RedisPipelineState _state

    def __init__(
            self, r: redis.Redis,
            get_script: Script, set_script: Script,
            min_ttl: int, max_ttl: int,
            sess: Optional[Session],
            max_keys_per_batch: int,
    ):
        self.client = r
        self.get_script = get_script
        self.set_script = set_script

        self._sess = sess or Session()

        self._min_ttl = min_ttl
        self._max_ttl = max_ttl

        self.max_keys_per_batch = max_keys_per_batch

        self._state = None

    cdef RedisPipelineState _get_state(self):
        if self._state is None:
            self._state = RedisPipelineState(self)
        return self._state

    def execute(self, state: RedisPipelineState):
        if not state.completed:
            state.execute()
            self._state = None

    def lease_get(self, str key):
        if self._state is None:
            self._state = RedisPipelineState(self)

        state = self._state

        index = len(state.keys)
        state.keys.append(key)

        # do init get result
        cdef _RedisGetResult result
        if len(_P) == 0:
            result = _RedisGetResult()
        else:
            result = _P.pop()

        result.pipe = self
        result.state = state
        result.index = index
        # end init get result

        return result

    def lease_set(self, key: str, cas: int, data: bytes) -> Promise[LeaseSetResponse]:
        state = self._get_state()

        ttl = random.randrange(self._min_ttl, self._max_ttl + 1)

        index = state.add_set_op(key=key, cas=cas, val=data, ttl=ttl)

        def lease_set_fn() -> LeaseSetResponse:
            self.execute(state)

            if state.redis_error is not None:
                return LeaseSetResponse(
                    status=LeaseSetStatus.ERROR,
                    error=f'Redis Set: {state.redis_error}'
                )

            set_resp = state.set_result[index]

            if set_resp == b'OK':
                status = LeaseSetStatus.OK
            elif set_resp == b'NF':
                status = LeaseSetStatus.NOT_FOUND
            else:
                status = LeaseSetStatus.CAS_MISMATCH

            return LeaseSetResponse(status=status)

        return lease_set_fn

    def delete(self, key: str) -> Promise[DeleteResponse]:
        cdef RedisPipelineState state = self._get_state()
        index = state.add_delete_op(key)

        def delete_fn() -> DeleteResponse:
            self.execute(state)
            if state.redis_error is not None:
                return DeleteResponse(
                    status=DeleteStatus.ERROR,
                    error=f'Redis Delete: {state.redis_error}'
                )

            resp = state.delete_result[index]
            status = DeleteStatus.OK if resp == 1 else DeleteStatus.NOT_FOUND
            return DeleteResponse(status=status)

        return delete_fn

    def lower_session(self) -> Session:
        return self._sess

    def finish(self) -> None:
        if self._state is not None:
            self.execute(self._state)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finish()


cdef class RedisClient:
    cdef:
        object _client
        object _get_script
        object _set_script
        int _min_ttl
        int _max_ttl
        int _max_keys_per_batch

    def __init__(
            self, object r,
            int min_ttl=6 * 3600, int max_ttl=12 * 3600,
            int max_keys_per_batch=100,
    ):
        self._client = r
        self._get_script = self._client.register_script(LEASE_GET_SCRIPT)
        self._set_script = self._client.register_script(LEASE_SET_SCRIPT)
        self._min_ttl = min_ttl
        self._max_ttl = max_ttl
        self._max_keys_per_batch = max_keys_per_batch

    cpdef object pipeline(self, object sess = None):
        return RedisPipeline(
            r=self._client,
            get_script=self._get_script,
            set_script=self._set_script,
            min_ttl=self._min_ttl,
            max_ttl=self._max_ttl,
            sess=sess,
            max_keys_per_batch=self._max_keys_per_batch,
        )