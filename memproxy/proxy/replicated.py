from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Optional, List, Tuple, Callable, Set

from .route import Selector, Stats


class ReplicatedSelector:
    _conf: _RouteConfig

    _chosen_server: Optional[int]
    _failed_servers: Set[int]

    def __init__(self, conf: _RouteConfig):
        self._conf = conf
        self._chosen_server = None

    def _compute_chosen_server(self) -> bool:
        remaining: List[int] = []
        weights: List[float] = []

        for server_id in self._conf.servers:
            usage = self._conf.stats.get_mem_usage(server_id)
            if usage is None:
                continue

            remaining.append(server_id)
            weights.append(usage)

        if len(remaining) == 0:
            # TODO
            pass

        recompute_weights_with_min_percent(weights, 1.0)

        # accumulate
        for i in range(1, len(weights)):
            weights[i] = weights[i - 1] + weights[i]

        max_weight = weights[-1]

        val = self._conf.rand(RAND_MAX)
        pos = float(val) / float(RAND_MAX)

        chosen_weight = max_weight * pos

        for i in range(len(weights)):
            if weights[i] > chosen_weight:
                self._chosen_server = remaining[i]
                return True

        self._chosen_server = remaining[-1]
        return False

    def set_failed_server(self, server_id: int) -> None:
        pass

    def select_server(self, _: str) -> Tuple[int, bool]:
        if not self._chosen_server:
            self._compute_chosen_server()

        assert self._chosen_server is not None
        return self._chosen_server, True

    def select_servers_for_delete(self) -> List[int]:
        result: List[int] = []
        for server_id in self._conf.servers:
            if server_id in self._failed_servers:
                continue
            result.append(server_id)
        return result

    def reset(self) -> None:
        self._chosen_server = None


RAND_MAX = 1_000_000
RandFunc = Callable[[int], int]  # (n) -> int, random from 0 -> n - 1


@dataclass
class _RouteConfig:
    servers: List[int]
    stats: Stats
    rand: RandFunc
    min_percent: float


class ReplicatedRoute:
    _conf: _RouteConfig

    def __init__(
            self, server_ids: List[int], stats: Stats,
            rand: RandFunc = random.randrange,
            min_percent: float = 1.0,
    ):
        if len(server_ids) == 0:
            raise ValueError("server_ids must not be empty")

        self._conf = _RouteConfig(
            servers=server_ids,
            stats=stats,
            rand=rand,
            min_percent=min_percent,
        )

    def new_selector(self) -> Selector:
        return ReplicatedSelector(conf=self._conf)


def recompute_weights_with_min_percent(weights: List[float], min_percent: float) -> None:
    total = 0.0
    for w in weights:
        total += w

    k = 0
    minimum = total * min_percent / 100
    for w in weights:
        if w < minimum:
            k += 1

    new_weight = total / (100.0 - float(k) * min_percent)
    for i in range(len(weights)):
        w = weights[i]
        if w < minimum:
            weights[i] = new_weight
