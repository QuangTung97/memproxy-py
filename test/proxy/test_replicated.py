import unittest
from typing import List

from memproxy.proxy import Route, ReplicatedRoute
from memproxy.proxy.replicated import RAND_MAX
from .fake_stats import StatsFake


class TestReplicatedSelector(unittest.TestCase):
    rand_calls: List[int]
    rand_val: int

    def setUp(self) -> None:
        self.servers = [21, 22, 23]
        self.stats = StatsFake()

        self.rand_calls = []
        self.rand_val = 0

        self.route: Route = ReplicatedRoute(self.servers, self.stats, rand=self.rand_func)
        self.selector = self.route.new_selector()

    def rand_func(self, n: int) -> int:
        self.rand_calls.append(n)
        return self.rand_val

    def test_normal(self) -> None:
        self.assertEqual(1000000, RAND_MAX)

        self.rand_val = 0
        self.stats.mem = {
            21: 100.0,
            22: 100.0,
            23: 100.0,
        }

        server_id, ok = self.selector.select_server('key01')
        self.assertEqual(21, server_id)
        self.assertEqual(True, ok)

        self.assertEqual([21, 22, 23], self.stats.get_calls)
        self.assertEqual([RAND_MAX], self.rand_calls)

        # call again
        server_id, ok = self.selector.select_server('key01')
        self.assertEqual(21, server_id)
        self.assertEqual(True, ok)

        self.assertEqual([21, 22, 23], self.stats.get_calls)
        self.assertEqual([RAND_MAX], self.rand_calls)

    def test_choose_servers(self) -> None:
        self.assertEqual(1000000, RAND_MAX)

        self.rand_val = 333334
        self.stats.mem = {
            21: 100.0,
            22: 100.0,
            23: 100.0,
        }

        server_id, ok = self.selector.select_server('key01')
        self.assertEqual(22, server_id)
        self.assertEqual(True, ok)

        self.assertEqual([21, 22, 23], self.stats.get_calls)
        self.assertEqual([RAND_MAX], self.rand_calls)

        # select again
        self.selector = self.route.new_selector()
        self.rand_val = 333333

        server_id, ok = self.selector.select_server('key01')
        self.assertEqual(21, server_id)
        self.assertEqual(True, ok)

        # select again
        self.selector = self.route.new_selector()
        self.rand_val = 666667

        server_id, ok = self.selector.select_server('key01')
        self.assertEqual(23, server_id)
        self.assertEqual(True, ok)

        self.assertEqual([21, 22, 23] * 3, self.stats.get_calls)
        self.assertEqual([RAND_MAX] * 3, self.rand_calls)
