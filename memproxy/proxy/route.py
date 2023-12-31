"""
Basic Date Types and Declaration of Protocols.
"""
from abc import abstractmethod
from typing import Tuple, Optional, List

from typing_extensions import Protocol


class Stats(Protocol):
    """A Protocol for getting memory usage from Redis servers."""

    @abstractmethod
    def get_mem_usage(self, server_id: int) -> Optional[float]:
        """
        :param server_id: server id
        :return: memory usage in bytes, return None if server not connected
        """

    @abstractmethod
    def notify_server_failed(self, server_id: int) -> None:
        """
        :param server_id: server id

        Notify server for fast detecting failed servers.
        """


class Selector(Protocol):
    """A Protocol for selecting cache servers to get & delete."""

    @abstractmethod
    def set_failed_server(self, server_id: int) -> None:
        """
        :param server_id: server id

        Set server id to not do get from in the next pipeline stage.
        """

    @abstractmethod
    def select_server(self, key: str) -> Tuple[int, bool]:
        """
        :param key: Redis cache key
        :return: a tuple includes server id and a boolean value
            whether that server id can be connected
        """

    @abstractmethod
    def select_servers_for_delete(self, key: str) -> List[int]:
        """
        :param key: Redis cache key
        :return: list of server ids for deletion
        """

    @abstractmethod
    def reset(self) -> None:
        """
        reset() is called whenever a pipeline stage is finished.
        """


# pylint: disable=too-few-public-methods
class Route(Protocol):
    """Object for selecting cache server for getting data."""

    @abstractmethod
    def new_selector(self) -> Selector:
        """Create a new selector, best use on request scope."""
