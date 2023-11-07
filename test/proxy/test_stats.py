import datetime
import time
import unittest

import redis

from memproxy.proxy import ServerStats


def mem_logger(server_id: int, mem: float):
    # print("USAGE =", server_id, mem, datetime.datetime.now())
    pass


class TestServerStatsSingleRedis(unittest.TestCase):
    stats: ServerStats

    def setUp(self) -> None:
        self.redis = redis.Redis()
        self.redis.flushall()
        self.redis.script_flush()

    def new_stats(self, duration: int):
        self.stats = ServerStats(clients={
            21: self.redis,
            22: self.redis,
        }, sleep_min=duration, sleep_max=duration,
            mem_logger=mem_logger,
        )

        self.addCleanup(self.stats.shutdown)

    def new_stats_one_failed(self, duration: int):
        self.stats = ServerStats(clients={
            21: self.redis,
            22: redis.Redis(port=6400),
        }, sleep_min=duration, sleep_max=duration,
            mem_logger=mem_logger,
        )

        self.addCleanup(self.stats.shutdown)

    def test_simple(self) -> None:
        self.new_stats(7)

        self.stats.notify_server_failed(21)
        self.stats.notify_server_failed(22)
        self.stats.notify_server_failed(23)

        time.sleep(0.2)

        print(self.stats.get_mem_usage(21))

        self.stats.notify_server_failed(22)
        time.sleep(0.1)

    def test_with_sleep(self) -> None:
        self.new_stats(1)
        time.sleep(1.5)

    def test_normal(self) -> None:
        self.skipTest('not running')

        self.new_stats(7)

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

    def test_one_failed(self) -> None:
        self.new_stats_one_failed(7)

        self.stats.notify_server_failed(21)
        self.stats.notify_server_failed(22)
        self.stats.notify_server_failed(23)

        time.sleep(0.2)

        v = self.stats.get_mem_usage(21)
        self.assertIsNotNone(v)
        print(v)

        self.assertEqual(None, self.stats.get_mem_usage(22))
