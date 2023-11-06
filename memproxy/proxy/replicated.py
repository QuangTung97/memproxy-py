from __future__ import annotations

import random
from typing import Optional, List, Tuple, Callable, Set

from .route import Selector, Stats


class ReplicatedSelector:
    _config_servers: List[int]
    _stats: Stats
    _rand: RandFunc

    _chosen_server: Optional[int]
    _failed_servers: Set[int]

    def __init__(self, server_ids: List[int], stats: Stats, rand: RandFunc):
        self._stats = stats
        self._config_servers = server_ids
        self._rand = rand

        self._chosen_server = None

    def _compute_chosen_server(self) -> bool:
        remaining: List[int] = []
        weights: List[float] = []

        for server_id in self._config_servers:
            usage = self._stats.get_mem_usage(server_id)
            if usage is None:
                continue

            remaining.append(server_id)
            weights.append(usage)

        if len(remaining) == 0:
            # TODO
            pass

        recompute_weights_with_min_percent(weights, 1.0)
        for i in range(1, len(weights)):
            weights[i] = weights[i - 1] + weights[i]

        max_weight = weights[-1]

        val = self._rand(RAND_MAX)
        pos = float(val) / float(RAND_MAX)

        chosen_weight = max_weight * pos

        for i in range(len(weights)):
            if weights[i] >= chosen_weight:
                self._chosen_server = remaining[i]
                break

        return True

    def set_failed_server(self, server_id: int) -> None:
        pass

    def select_server(self, _: str) -> Tuple[int, bool]:
        if not self._chosen_server:
            self._compute_chosen_server()

        assert self._chosen_server is not None
        return self._chosen_server, True

    def select_servers_for_delete(self) -> List[int]:
        result: List[int] = []
        for server_id in self._config_servers:
            if server_id in self._failed_servers:
                continue
            result.append(server_id)
        return result


RAND_MAX = 1_000_000
RandFunc = Callable[[int], int]  # (n) -> int, random from 0 -> n - 1


class ReplicatedRoute:
    _config_servers: List[int]
    _stats: Stats
    _rand: RandFunc

    def __init__(self, server_ids: List[int], stats: Stats, rand: RandFunc = random.randrange):
        if len(server_ids) == 0:
            raise ValueError("server_ids must not be empty")

        self._config_servers = server_ids
        self._stats = stats
        self._rand = rand

    def new_selector(self) -> Selector:
        return ReplicatedSelector(
            server_ids=self._config_servers,
            stats=self._stats,
            rand=self._rand,
        )


def recompute_weights_with_min_percent(weights: List[float], min_percent: float) -> None:
    pass
