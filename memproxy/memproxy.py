from abc import abstractmethod
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


class LeaseSetStatus(Enum):
    OK = 1
    ERROR = 2
    NOT_FOUND = 3  # key not found
    CAS_MISMATCH = 4


@dataclass
class LeaseSetResponse:
    status: LeaseSetStatus
    error: Optional[str] = None


class DeleteStatus(Enum):
    OK = 1
    ERROR = 2
    NOT_FOUND = 3  # key not found


@dataclass
class DeleteResponse:
    status: DeleteStatus
    error: Optional[str] = None


class Pipeline(Protocol):
    @abstractmethod
    def lease_get(self, key: str) -> Promise[LeaseGetResponse]: pass

    @abstractmethod
    def lease_set(self, key: str, cas: int, data: bytes) -> Promise[LeaseSetResponse]: pass

    @abstractmethod
    def delete(self, key: str) -> Promise[DeleteResponse]: pass

    @abstractmethod
    def lower_session(self) -> Session: pass

    @abstractmethod
    def finish(self) -> None: pass

    @abstractmethod
    def __enter__(self): pass

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb): pass


class CacheClient(Protocol):
    @abstractmethod
    def pipeline(self, sess: Optional[Session] = None) -> Pipeline: pass
