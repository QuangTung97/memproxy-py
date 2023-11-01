from typing import List, Optional

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
        redis.call('SET', k, cas_str)
        result[i] = cas_str
    end
end

return result
"""

LEASE_SET_SCRIPT = """
local result = {}

return result
"""


class RedisPipelineState:
    keys: List[str]
    get_result: List[bytes]
    completed: bool

    def __init__(self):
        self.keys = []
        self.completed = False

    def add_get_key(self, key: str) -> int:
        index = len(self.keys)
        self.keys.append(key)
        return index

    def execute(self, get_script: Script, client: redis.Redis):
        script_resp = get_script(keys=self.keys, client=client)
        state.get_result = script_resp


CAS_PREFIX = b'cas:'


class RedisPipeline:
    _client: redis.Redis
    _get_script: Script
    _state: Optional[RedisPipelineState]

    def __init__(self, r: redis.Redis, get_script: Script):
        self._client = r
        self._get_script = get_script
        self._state = None

    def _get_state(self) -> RedisPipelineState:
        if self._state is None:
            self._state = RedisPipelineState()
        return self._state

    def lease_get(self, key: str) -> Promise[LeaseGetResponse]:
        state = self._get_state()

        index = state.add_get_key(key)

        def lease_get_fn() -> LeaseGetResponse:
            if not state.completed:
                self._state = None
                state.completed = True


            get_resp = state.get_result[index]
            if get_resp.startswith(CAS_PREFIX):
                # TODO Check error
                cas = int(get_resp[len(CAS_PREFIX):])
                return LeaseGetResponse(
                    status=LeaseGetStatus.LEASE_GRANTED,
                    cas=cas,
                    data=bytes(),
                )
            else:
                return LeaseGetResponse(
                    status=LeaseGetStatus.FOUND,
                    cas=0,
                    data=bytes(get_resp),
                )

        return lease_get_fn

    def lease_set(self, key: str, cas: int, data: bytes) -> Promise[LeaseSetResponse]:
        raise NotImplementedError()

    def delete(self) -> Promise[DeleteResponse]:
        raise NotImplementedError()

    def lower_session(self) -> Session:
        raise NotImplementedError()


class RedisClient:
    _client: redis.Redis
    _get_script: Script

    def __init__(self, r: redis.Redis):
        self._client = r
        self._get_script = self._client.register_script(LEASE_GET_SCRIPT)

    def pipeline(self) -> Pipeline:
        return RedisPipeline(self._client, self._get_script)


def _redis_client_type_check(redis_client: RedisClient):
    _client: CacheClient = redis_client
