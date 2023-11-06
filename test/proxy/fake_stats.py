from typing import Set, List, Optional, Dict


class StatsFake:
    failed_servers: Set[int]

    get_calls: List[int]
    notify_calls: List[int]

    mem: Dict[int, float]

    def __init__(self):
        self.failed_servers = set()

        self.get_calls = []
        self.notify_calls = []

        self.mem = {}

    def get_mem_usage(self, server_id: int) -> Optional[float]:
        self.get_calls.append(server_id)

        if server_id in self.failed_servers:
            return server_id

        return self.mem[server_id]

    def notify_server_failed(self, server_id: int) -> None:
        self.notify_calls.append(server_id)
        self.failed_servers.add(server_id)
