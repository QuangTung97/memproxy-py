from dataclasses import dataclass
from typing import List, Optional, Union

import redis
from redis.commands.core import Script

from .memproxy import CacheClient, Pipeline, Promise
from .memproxy import LeaseGetStatus, LeaseGetResponse, LeaseSetResponse, DeleteResponse
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
    
    local cas_str = 'cas:' .. ARGV[i * 2 - 1]
    local val = 'val:' .. ARGV[i * 2]
    
    if resp and resp == cas_str then
        redis.call('SET', k, val, 'EX', 30)
        result[i] = 'OK'
    else
        result[i] = 'NF'
    end
end

return result
"""


@dataclass
class SetInput:
    key: str
    cas: int
    val: bytes


class RedisPipelineState:
    completed: bool

    _keys: List[str]
    get_result: List[bytes]

    _set_inputs: List[SetInput]
    set_result: List[bytes]

    def __init__(self):
        self._keys = []
        self.completed = False
        self._set_inputs = []

    def add_get_op(self, key: str) -> int:
        index = len(self._keys)
        self._keys.append(key)
        return index

    def add_set_op(self, key: str, cas: int, val: bytes) -> int:
        index = len(self._set_inputs)
        self._set_inputs.append(SetInput(key=key, cas=cas, val=val))
        return index

    def execute(self, get_script: Script, set_script: Script, client: redis.Redis):
        if len(self._keys) > 0:
            self.get_result = get_script(keys=self._keys, client=client)

        if len(self._set_inputs) > 0:
            keys = [i.key for i in self._set_inputs]

            args: List[Union[int, bytes]] = []
            for i in self._set_inputs:
                args.append(i.cas)
                args.append(i.val)

            self.set_result = set_script(keys=keys, args=args, client=client)

        self.completed = True


CAS_PREFIX = b'cas:'
VAL_PREFIX = b'val:'


class RedisPipeline:
    _client: redis.Redis
    _get_script: Script
    _set_script: Script
    _state: Optional[RedisPipelineState]

    def __init__(self, r: redis.Redis, get_script: Script, set_script: Script):
        self._client = r
        self._get_script = get_script
        self._set_script = set_script
        self._state = None

    def _get_state(self) -> RedisPipelineState:
        if self._state is None:
            self._state = RedisPipelineState()
        return self._state

    def _execute(self, state: RedisPipelineState):
        if not state.completed:
            state.execute(self._get_script, self._set_script, self._client)
            self._state = None

    def lease_get(self, key: str) -> Promise[LeaseGetResponse]:
        state = self._get_state()
        index = state.add_get_op(key)

        def lease_get_fn() -> LeaseGetResponse:
            self._execute(state)

            get_resp = state.get_result[index]
            if get_resp.startswith(CAS_PREFIX):
                num_str = get_resp[len(CAS_PREFIX):].decode()
                if num_str.isnumeric():
                    pass  # TODO Error

                cas = int(num_str)
                return LeaseGetResponse(
                    status=LeaseGetStatus.LEASE_GRANTED,
                    cas=cas,
                    data=bytes(),
                )
            else:
                get_val = get_resp
                if len(get_val) > len(VAL_PREFIX):
                    get_val = get_resp[len(VAL_PREFIX):]

                return LeaseGetResponse(
                    status=LeaseGetStatus.FOUND,
                    cas=0,
                    data=get_val,
                )

        return lease_get_fn

    def lease_set(self, key: str, cas: int, data: bytes) -> Promise[LeaseSetResponse]:
        state = self._get_state()
        state.add_set_op(key, cas, data)

        def lease_set_fn() -> LeaseSetResponse:
            self._execute(state)
            return LeaseSetResponse()

        return lease_set_fn

    def delete(self) -> Promise[DeleteResponse]:
        raise NotImplementedError()

    def lower_session(self) -> Session:
        raise NotImplementedError()


class RedisClient:
    _client: redis.Redis
    _get_script: Script
    _set_script: Script

    def __init__(self, r: redis.Redis):
        self._client = r
        self._get_script = self._client.register_script(LEASE_GET_SCRIPT)
        self._set_script = self._client.register_script(LEASE_SET_SCRIPT)

    def pipeline(self) -> Pipeline:
        return RedisPipeline(self._client, self._get_script, self._set_script)


def _redis_client_type_check(redis_client: RedisClient):
    _client: CacheClient = redis_client
