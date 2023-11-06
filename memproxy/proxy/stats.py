import datetime
import logging
import random
import threading
import time
from typing import Optional, Dict, List, Set

import redis


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
        d = random.randint(sleep_min, sleep_max)
        self.next_wake_up = time.time() + float(d)

    def get_mem_usage(self):
        try:
            usage = self.client.info('memory').get('used_memory')
            print("GET USAGE =", usage, datetime.datetime.now())
            self.mem = usage
        except Exception as e:
            self.mem = None
            self.error = str(e)
            logging.error(f'Server Stats error. {str(e)}')


class ServerStats:
    _servers: List[int]
    _states: Dict[int, _ServerState]

    _sleep_min: int
    _sleep_max: int

    _mut: threading.Lock
    _notified: Set[int]
    _closed: bool
    _cond: threading.Condition

    _finished: threading.Semaphore

    def __init__(self, clients: Dict[int, redis.Redis], sleep_min: int = 150, sleep_max: int = 300):
        self._sleep_min = sleep_min
        self._sleep_max = sleep_max

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
            state.get_mem_usage()
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

    def _clear_notified(self):
        now = time.time()
        for server_id in self._servers:
            state = self._states[server_id]
            if state.next_wake_up <= now:
                state.notified = False
                self._notified.add(server_id)

    def _notify_servers(self, servers: List[int]):
        for server_id in servers:
            state = self._states[server_id]
            if state.notified:
                continue

            state.notified = True

            state.get_mem_usage()
            state.compute_next_wake_up(sleep_min=self._sleep_min, sleep_max=self._sleep_max)

    def _do_wait(self):
        min_wake_up = self._find_min_wake_up()

        now = time.time()
        timeout = min_wake_up - now
        print("TIMEOUT:", timeout, datetime.datetime.now())
        if timeout < 0:
            timeout = 0

        self._cond.wait(timeout=timeout)
        self._clear_notified()

    def _run(self):
        while True:
            with self._mut:
                while len(self._notified) == 0 and not self._closed:
                    self._do_wait()

                if self._closed:
                    self._finished.release()
                    return

                notify_list = [server_id for server_id in self._notified]
                self._notified.clear()

            self._notify_servers(notify_list)

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
