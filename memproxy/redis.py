from .memproxy import CacheClient, Pipeline, Promise, LeaseGetResponse, LeaseSetResponse, DeleteResponse
from .session import Session


class RedisPipeline:
    def lease_get(self, key: str) -> Promise[LeaseGetResponse]:
        raise NotImplementedError()

    def lease_set(self, key: str, cas: int, data: bytes) -> Promise[LeaseSetResponse]:
        raise NotImplementedError()

    def delete(self) -> Promise[DeleteResponse]:
        raise NotImplementedError()

    def lower_session(self) -> Session:
        raise NotImplementedError()

    def __init__(self):
        pass


class RedisClient:
    def __init__(self):
        pass

    def pipeline(self) -> Pipeline:
        raise NotImplementedError()


def _redis_client_type_check(redis_client: RedisClient):
    _client: CacheClient = redis_client
