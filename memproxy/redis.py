from __future__ import annotations

import random
from dataclasses import dataclass
from typing import List, Optional, Union

import redis
from redis.commands.core import Script

from .memproxy import LeaseGetResponse, LeaseSetResponse, DeleteResponse
from .memproxy import LeaseGetStatus, LeaseSetStatus, DeleteStatus
from .memproxy import Pipeline, Promise
from .session import Session

LEASE_GET_SCRIPT = """
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

LEASE_SET_SCRIPT = """
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


@dataclass
class SetInput:
    key: str
    cas: int
    val: bytes
    ttl: int


class RedisPipelineState:
    _pipe: RedisPipeline
    completed: bool

    _keys: List[str]
    get_result: List[bytes]

    _set_inputs: List[SetInput]
    set_result: List[bytes]

    _delete_keys: List[str]
    delete_result: List[int]

    redis_error: Optional[str]

    def __init__(self, pipe: RedisPipeline):
        self._pipe = pipe
        self.completed = False

        self._keys = []
        self._set_inputs = []
        self._delete_keys = []

        self.redis_error = None

    def add_get_op(self, key: str) -> int:
        index = len(self._keys)
        self._keys.append(key)
        return index

    def add_set_op(self, key: str, cas: int, val: bytes, ttl: int) -> int:
        index = len(self._set_inputs)
        self._set_inputs.append(SetInput(key=key, cas=cas, val=val, ttl=ttl))
        return index

    def add_delete_op(self, key: str) -> int:
        index = len(self._delete_keys)
        self._delete_keys.append(key)
        return index

    def execute(self) -> None:
        try:
            self._execute_in_try()
        except Exception as e:
            self.redis_error = str(e)

    def _execute_in_try(self) -> None:
        if len(self._keys) > 0:
            self._execute_lease_get()

        if len(self._set_inputs) > 0:
            self._execute_lease_set()

        if len(self._delete_keys):
            with self._pipe.client.pipeline(transaction=False) as pipe:
                for key in self._delete_keys:
                    pipe.delete(key)
                self.delete_result = pipe.execute()

        self.completed = True

    def _execute_lease_get(self) -> None:
        batch_size = self._pipe.max_keys_per_batch

        if len(self._keys) <= batch_size:
            self.get_result = self._pipe.get_script(keys=self._keys, client=self._pipe.client)
            return

        with self._pipe.client.pipeline(transaction=False) as pipe:
            for n in range(0, len(self._keys), batch_size):
                keys = self._keys[n: n + batch_size]
                self._pipe.get_script(keys=keys, client=pipe)
            pipe_result = pipe.execute()

        self.get_result = []
        for r in pipe_result:
            self.get_result.extend(r)

    def _execute_lease_set(self) -> None:
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

    def _execute_lease_set_inputs(self, inputs: List[SetInput], client) -> List[bytes]:
        keys = [i.key for i in inputs]

        args: List[Union[int, bytes]] = []
        for i in inputs:
            args.append(i.cas)
            args.append(i.val)
            args.append(i.ttl)

        return self._pipe.set_script(keys=keys, args=args, client=client)


CAS_PREFIX = b'cas:'
VAL_PREFIX = b'val:'


class RedisPipeline:
    client: redis.Redis
    get_script: Script
    set_script: Script

    _sess: Session

    _min_ttl: int
    _max_ttl: int

    max_keys_per_batch: int

    _state: Optional[RedisPipelineState]

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

    def _get_state(self) -> RedisPipelineState:
        if self._state is None:
            self._state = RedisPipelineState(self)
        return self._state

    def _execute(self, state: RedisPipelineState):
        if not state.completed:
            state.execute()
            self._state = None

    def lease_get(self, key: str) -> Promise[LeaseGetResponse]:
        state = self._get_state()
        index = state.add_get_op(key)

        def lease_get_fn() -> LeaseGetResponse:
            self._execute(state)

            if state.redis_error is not None:
                return LeaseGetResponse(
                    status=LeaseGetStatus.ERROR,
                    cas=0,
                    data=b'',
                    error=f'Redis Get: {state.redis_error}'
                )

            get_resp = state.get_result[index]
            if get_resp.startswith(CAS_PREFIX):
                num_str = get_resp[len(CAS_PREFIX):].decode()
                if not num_str.isnumeric():
                    return LeaseGetResponse(
                        status=LeaseGetStatus.ERROR,
                        cas=0,
                        data=b'',
                        error=f'Value "{num_str}" is not a number'
                    )

                cas = int(num_str)
                return LeaseGetResponse(
                    status=LeaseGetStatus.LEASE_GRANTED,
                    cas=cas,
                    data=b'',
                )
            else:
                get_val = get_resp
                if get_val.startswith(VAL_PREFIX):
                    get_val = get_resp[len(VAL_PREFIX):]

                return LeaseGetResponse(
                    status=LeaseGetStatus.FOUND,
                    cas=0,
                    data=get_val,
                )

        return lease_get_fn

    def lease_set(self, key: str, cas: int, data: bytes) -> Promise[LeaseSetResponse]:
        state = self._get_state()

        ttl = random.randrange(self._min_ttl, self._max_ttl + 1)

        index = state.add_set_op(key=key, cas=cas, val=data, ttl=ttl)

        def lease_set_fn() -> LeaseSetResponse:
            self._execute(state)

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
        state = self._get_state()
        index = state.add_delete_op(key)

        def delete_fn() -> DeleteResponse:
            self._execute(state)
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
            self._execute(self._state)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finish()


class RedisClient:
    _client: redis.Redis
    _get_script: Script
    _set_script: Script
    _min_ttl: int
    _max_ttl: int
    _max_keys_per_batch: int

    def __init__(
            self, r: redis.Redis,
            min_ttl=6 * 3600, max_ttl=12 * 3600,
            max_keys_per_batch=100,
    ):
        self._client = r
        self._get_script = self._client.register_script(LEASE_GET_SCRIPT)
        self._set_script = self._client.register_script(LEASE_SET_SCRIPT)
        self._min_ttl = min_ttl
        self._max_ttl = max_ttl
        self._max_keys_per_batch = max_keys_per_batch

    def pipeline(self, sess: Optional[Session] = None) -> Pipeline:
        return RedisPipeline(
            r=self._client,
            get_script=self._get_script,
            set_script=self._set_script,
            min_ttl=self._min_ttl,
            max_ttl=self._max_ttl,
            sess=sess,
            max_keys_per_batch=self._max_keys_per_batch,
        )
