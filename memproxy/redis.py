from __future__ import annotations

import random
from dataclasses import dataclass
from typing import List, Optional, Union

import redis
from redis.commands.core import Script

from .memproxy import LeaseGetStatus, LeaseGetResponse, LeaseSetResponse, DeleteResponse
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
    
    if resp and resp == cas_str then
        redis.call('SET', k, val, 'EX', ttl)
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
    ttl: int


class RedisPipelineState:
    _pipe: RedisPipeline

    completed: bool

    _keys: List[str]
    get_result: List[bytes]

    _set_inputs: List[SetInput]
    set_result: List[bytes]

    _delete_keys: List[str]

    def __init__(self, pipe: RedisPipeline):
        self._pipe = pipe
        self._keys = []
        self.completed = False
        self._set_inputs = []
        self._delete_keys = []

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
        if len(self._keys) > 0:
            self.get_result = self._pipe.get_script(keys=self._keys, client=self._pipe.client)

        if len(self._set_inputs) > 0:
            keys = [i.key for i in self._set_inputs]

            args: List[Union[int, bytes]] = []
            for i in self._set_inputs:
                args.append(i.cas)
                args.append(i.val)
                args.append(i.ttl)

            self.set_result = self._pipe.set_script(keys=keys, args=args, client=self._pipe.client)

        if len(self._delete_keys):
            self._pipe.client.delete(*self._delete_keys)

        self.completed = True


CAS_PREFIX = b'cas:'
VAL_PREFIX = b'val:'


class RedisPipeline:
    client: redis.Redis
    get_script: Script
    set_script: Script

    _sess: Session

    _min_ttl: int
    _max_ttl: int

    _state: Optional[RedisPipelineState]

    def __init__(self, r: redis.Redis, get_script: Script, set_script: Script, min_ttl: int, max_ttl: int):
        self.client = r
        self.get_script = get_script
        self.set_script = set_script

        self._sess = Session()

        self._min_ttl = min_ttl
        self._max_ttl = max_ttl

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

            get_resp = state.get_result[index]
            if get_resp.startswith(CAS_PREFIX):
                num_str = get_resp[len(CAS_PREFIX):].decode()
                if not num_str.isnumeric():
                    return LeaseGetResponse(
                        status=LeaseGetStatus.ERROR,
                        cas=0,
                        data=b'',
                        error=f'value "{num_str}" is not a number'
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
        state.add_set_op(key=key, cas=cas, val=data, ttl=ttl)

        def lease_set_fn() -> LeaseSetResponse:
            self._execute(state)
            return LeaseSetResponse()

        return lease_set_fn

    def delete(self, key: str) -> Promise[DeleteResponse]:
        state = self._get_state()
        state.add_delete_op(key)

        def delete_fn() -> DeleteResponse:
            self._execute(state)
            return DeleteResponse()

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

    def __init__(self, r: redis.Redis, min_ttl=6 * 3600, max_ttl=12 * 3600):
        self._client = r
        self._get_script = self._client.register_script(LEASE_GET_SCRIPT)
        self._set_script = self._client.register_script(LEASE_SET_SCRIPT)
        self._min_ttl = min_ttl
        self._max_ttl = max_ttl

    def pipeline(self) -> Pipeline:
        return RedisPipeline(
            r=self._client,
            get_script=self._get_script,
            set_script=self._set_script,
            min_ttl=self._min_ttl,
            max_ttl=self._max_ttl,
        )
