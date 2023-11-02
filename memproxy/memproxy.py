from dataclasses import dataclass
from enum import Enum
from typing import Protocol, Callable, TypeVar, Optional

from .session import Session

T = TypeVar("T")

Promise = Callable[[], T]


class LeaseGetStatus(Enum):
    FOUND = 1
    LEASE_GRANTED = 2
    ERROR = 3


@dataclass
class LeaseGetResponse:
    status: LeaseGetStatus
    data: bytes
    cas: int
    error: Optional[str] = None


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

    def delete(self, key: str) -> Promise[DeleteResponse]:
        raise NotImplementedError()

    def lower_session(self) -> Session:
        raise NotImplementedError()

    def finish(self) -> None:
        raise NotImplementedError()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finish()


class CacheClient(Protocol):
    def pipeline(self) -> Pipeline:
        raise NotImplementedError()
