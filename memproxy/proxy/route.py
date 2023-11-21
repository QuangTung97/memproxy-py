from abc import abstractmethod
from typing import Tuple, Optional, List
from typing_extensions import Protocol


class Stats(Protocol):
    @abstractmethod
    def get_mem_usage(self, server_id: int) -> Optional[float]: pass

    @abstractmethod
    def notify_server_failed(self, server_id: int) -> None: pass


class Selector(Protocol):
    @abstractmethod
    def set_failed_server(self, server_id: int) -> None: pass

    @abstractmethod
    def select_server(self, key: str) -> Tuple[int, bool]: pass

    @abstractmethod
    def select_servers_for_delete(self, key: str) -> List[int]: pass

    @abstractmethod
    def reset(self) -> None: pass


class Route(Protocol):
    @abstractmethod
    def new_selector(self) -> Selector: pass
