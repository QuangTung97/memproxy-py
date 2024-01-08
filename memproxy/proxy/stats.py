import logging
import random
import threading
import time
from typing import Optional, Dict, List, Set, Callable

import redis

MemLogger = Callable[[int, float], None]


class _ServerState:
    client: redis.Redis

    next_wake_up: float
    notified: bool

    error: Optional[str]

    mem: Optional[float]

    def __init__(self, r: redis.Redis):
        self.client = r

        self.notified = False

        self.error = None
        self.mem = 0

    def compute_next_wake_up(self, sleep_min: int, sleep_max: int):
        rand = random.Random(time.time_ns())
        d = rand.randint(sleep_min, sleep_max)
        self.next_wake_up = time.time() + float(d)

    def get_mem_usage(self, server_id, mem_logger: MemLogger):
        try:
            usage = self.client.info('memory').get('used_memory')
            if usage:
                self.mem = usage
                mem_logger(server_id, usage)
        except Exception as e:
            self.mem = None
            self.error = str(e)
            logging.error(f'Server Stats error. {str(e)}')


def _empty_logger(_server_id: int, _mem: float):
    pass


class ServerStats:
    _servers: List[int]
    _states: Dict[int, _ServerState]

    _sleep_min: int
    _sleep_max: int
    _mem_logger: MemLogger

    _mut: threading.Lock
    _notified: Set[int]
    _closed: bool
    _cond: threading.Condition

    _finished: threading.Semaphore

    def __init__(
            self, clients: Dict[int, redis.Redis],
            sleep_min: int = 150, sleep_max: int = 300,
            mem_logger: MemLogger = _empty_logger,
    ):
        self._sleep_min = sleep_min
        self._sleep_max = sleep_max
        self._mem_logger = mem_logger

        self._mut = threading.Lock()
        self._notified = set()
        self._closed = False
        self._cond = threading.Condition(lock=self._mut)

        self._finished = threading.Semaphore(value=0)

        servers = [server_id for server_id in clients]
        servers.sort()
        self._servers = servers

        self._states = {}
        for server_id in self._servers:
            r = clients[server_id]
            state = _ServerState(r=r)

            self._states[server_id] = state
            state.get_mem_usage(server_id, self._mem_logger)
            state.compute_next_wake_up(sleep_min=self._sleep_min, sleep_max=self._sleep_max)

        threading.Thread(target=self._run, daemon=True).start()

    def _find_min_wake_up(self) -> float:
        first_server = self._servers[0]

        min_wake_up = self._states[first_server].next_wake_up

        for server_id in self._servers[1:]:
            wake_up = self._states[server_id].next_wake_up
            if wake_up < min_wake_up:
                min_wake_up = wake_up

        return min_wake_up

    def _notify_timeout(self) -> List[int]:
        timeout_servers: List[int] = []

        now = time.time()
        for server_id in self._servers:
            state = self._states[server_id]
            if state.next_wake_up <= now:
                state.notified = False
                timeout_servers.append(server_id)

        return timeout_servers

    def _notify_servers(self, servers: List[int], is_timeout: bool):
        for server_id in servers:
            state = self._states.get(server_id)
            if not state:
                continue

            if not is_timeout:
                if state.notified:
                    continue
                state.notified = True

            state.get_mem_usage(server_id, self._mem_logger)
            state.compute_next_wake_up(sleep_min=self._sleep_min, sleep_max=self._sleep_max)

    def _do_wait(self) -> List[int]:
        min_wake_up = self._find_min_wake_up()

        timeout = min_wake_up - time.time()
        if timeout > 0:
            self._cond.wait(timeout=timeout)

        return self._notify_timeout()

    def _run(self) -> None:
        while True:
            timeout_servers: List[int] = []
            with self._mut:
                while len(self._notified) == 0 and len(timeout_servers) == 0 and not self._closed:
                    timeout_servers = self._do_wait()

                if self._closed:
                    self._finished.release()
                    return

                notify_list = [server_id for server_id in self._notified]
                self._notified.clear()

            self._notify_servers(timeout_servers, True)
            self._notify_servers(notify_list, False)

    def get_mem_usage(self, server_id: int) -> Optional[float]:
        return self._states[server_id].mem

    def notify_server_failed(self, server_id: int) -> None:
        with self._mut:
            self._notified.add(server_id)
            self._cond.notify()

    def shutdown(self):
        with self._mut:
            self._closed = True
            self._cond.notify()
        self._finished.acquire()
