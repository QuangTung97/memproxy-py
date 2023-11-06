import time
import unittest

import redis

from memproxy.proxy import ServerStats


class TestServerStatsSingleRedis(unittest.TestCase):
    def setUp(self) -> None:
        self.redis = redis.Redis()
        self.redis.flushall()
        self.redis.script_flush()

        duration = 100

        self.stats = ServerStats(clients={
            21: self.redis,
        }, sleep_min=duration, sleep_max=duration)

        self.addCleanup(self.stats.shutdown)

    def test_normal(self) -> None:
        self.skipTest('not running')

        v = self.stats.get_mem_usage(21)

        time.sleep(3)

        self.stats.notify_server_failed(21)
        time.sleep(1)
        self.stats.notify_server_failed(21)

        v = self.stats.get_mem_usage(21)
        print(v)

        time.sleep(3)
