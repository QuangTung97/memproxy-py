import datetime
import time
import unittest

import redis

from memproxy.proxy import ServerStats


def mem_logger(server_id: int, mem: float):
    print("USAGE =", server_id, mem, datetime.datetime.now())


class TestServerStatsSingleRedis(unittest.TestCase):
    stats: ServerStats

    def setUp(self) -> None:
        self.redis = redis.Redis()
        self.redis.flushall()
        self.redis.script_flush()

        duration = 7

        self.stats = ServerStats(clients={
            21: self.redis,
            22: self.redis,
        }, sleep_min=duration, sleep_max=duration,
            mem_logger=mem_logger,
        )

        self.addCleanup(self.stats.shutdown)

    def test_simple(self) -> None:
        self.stats.notify_server_failed(21)
        self.stats.notify_server_failed(22)
        time.sleep(0.1)

    def test_normal(self) -> None:
        self.skipTest('not running')
        print("START", datetime.datetime.now())
        time.sleep(3)

        self.stats.notify_server_failed(21)
        self.stats.notify_server_failed(22)
        self.stats.notify_server_failed(23)

        time.sleep(1)
        self.stats.notify_server_failed(21)
        self.stats.notify_server_failed(22)

        v = self.stats.get_mem_usage(21)
        print("MEMORY:", v)

        time.sleep(15)
        self.stats.notify_server_failed(21)
        time.sleep(1)
        self.stats.notify_server_failed(21)
