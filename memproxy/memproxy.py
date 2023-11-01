from dataclasses import dataclass
from enum import Enum
from typing import Protocol, Callable, TypeVar

from .session import Session

T = TypeVar("T")

Promise = Callable[[], T]


class LeaseGetStatus(Enum):
    FOUND = 1
    LEASE_GRANTED = 2


@dataclass
class LeaseGetResponse:
    status: LeaseGetStatus
    data: bytes
    cas: int


@dataclass
class LeaseSetResponse:
    pass


@dataclass
class DeleteResponse:
    pass


class Pipeline(Protocol):
    def lease_get(self, key: str) -> Promise[LeaseGetResponse]:
        raise NotImplementedError()

    def lease_set(self, key: str, cas: int, data: bytes) -> Promise[LeaseSetResponse]:
        raise NotImplementedError()

    def delete(self) -> Promise[DeleteResponse]:
        raise NotImplementedError()

    def lower_session(self) -> Session:
        raise NotImplementedError()


class CacheClient(Protocol):
    def pipeline(self) -> Pipeline:
        raise NotImplementedError()
