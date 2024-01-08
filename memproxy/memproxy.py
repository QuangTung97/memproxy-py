"""
Basic Data Types of Memproxy.
"""
from abc import abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Callable, TypeVar, Optional, Tuple

from typing_extensions import Protocol

from .session import Session

T = TypeVar("T")

Promise = Callable[[], T]

# status, data, cas, error
# status = 1 (OK)
# status = 2 (LEASE_GRANTED)
# status = 3 (ERROR)
LeaseGetResponse = Tuple[int, bytes, int, Optional[str]]


class LeaseSetStatus(Enum):
    """Status when calling Pipeline.lease_set()."""
    OK = 1
    ERROR = 2
    NOT_FOUND = 3  # key not found
    CAS_MISMATCH = 4


@dataclass
class LeaseSetResponse:
    """Response Object when calling Pipeline.lease_set()."""
    status: LeaseSetStatus
    error: Optional[str] = None


class DeleteStatus(Enum):
    """Status when calling Pipeline.delete()."""
    OK = 1
    ERROR = 2
    NOT_FOUND = 3  # key not found


@dataclass
class DeleteResponse:
    """Response Object when calling Pipeline.delete()."""
    status: DeleteStatus
    error: Optional[str] = None


# pylint: disable=too-few-public-methods
class LeaseGetResult(Protocol):
    """Response Object when calling Pipeline.lease_get()."""

    @abstractmethod
    def result(self) -> LeaseGetResponse:
        """When call will return the lease get response object."""


# pylint: disable=too-few-public-methods
class LeaseGetResultFunc:
    """Mostly for testing purpose."""

    _fn: Promise[LeaseGetResponse]

    def __init__(self, fn: Promise[LeaseGetResponse]):
        self._fn = fn

    def result(self) -> LeaseGetResponse:
        """Return lease get result."""
        return self._fn()


class Pipeline(Protocol):
    """A Cache Pipeline."""

    @abstractmethod
    def lease_get(self, key: str) -> LeaseGetResult:
        """Returns data or a cas (lease id) number when not found."""

    @abstractmethod
    def lease_set(self, key: str, cas: int, data: bytes) -> Promise[LeaseSetResponse]:
        """Set data for the key when cas number is matched."""

    @abstractmethod
    def delete(self, key: str) -> Promise[DeleteResponse]:
        """Delete key from cache servers."""

    @abstractmethod
    def lower_session(self) -> Session:
        """Returns a session with lower priority."""

    @abstractmethod
    def finish(self) -> None:
        """Do clean up, for example, flush pending operations, e.g. set, delete."""

    @abstractmethod
    def __enter__(self):
        """Do clean up but using with."""

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Do clean up but using with."""


class CacheClient(Protocol):
    """Cache Client is a class to create Pipeline objects."""

    @abstractmethod
    def pipeline(self, sess: Optional[Session] = None) -> Pipeline:
        """Create a new pipeline, create a new Session if input sess is None."""
