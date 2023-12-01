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

        self.route: Route = ReplicatedRoute(self.servers, self.stats, rand=self.rand_factory)
        self.selector = self.route.new_selector()

    def rand_factory(self):
        return self.rand_func

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
        self.selector.reset()
        self.rand_val = 666667

        server_id, ok = self.selector.select_server('key01')
        self.assertEqual(23, server_id)
        self.assertEqual(True, ok)

        self.assertEqual([21, 22, 23] * 3, self.stats.get_calls)
        self.assertEqual([RAND_MAX] * 3, self.rand_calls)

    def test_choose_servers_with_min_percent(self) -> None:
        self.rand_val = 494999
        self.stats.mem = {
            21: 100.0,
            22: 0.0,  # -> x = 2.02
            23: 100.0,
        }

        server_id, ok = self.selector.select_server('key01')
        self.assertEqual(21, server_id)
        self.assertEqual(True, ok)

        # select again
        self.selector.reset()
        self.rand_val = 495000

        server_id, ok = self.selector.select_server('key01')
        self.assertEqual(22, server_id)
        self.assertEqual(True, ok)

        # select again
        self.selector.reset()
        self.rand_val = 505000

        server_id, ok = self.selector.select_server('key01')
        self.assertEqual(23, server_id)
        self.assertEqual(True, ok)

        # select again
        self.selector.reset()
        self.rand_val = 504999

        server_id, ok = self.selector.select_server('key01')
        self.assertEqual(22, server_id)
        self.assertEqual(True, ok)

    def test_choose_servers_with_multiple_min_percent(self) -> None:
        self.rand_val = 980000
        self.stats.mem = {
            21: 1000.0,
            22: 0.0,  # -> x 10.204
            23: 0.0,
        }

        server_id, ok = self.selector.select_server('key01')
        self.assertEqual(21, server_id)
        self.assertEqual(True, ok)

        # select again
        self.selector.reset()
        self.rand_val = 980001

        server_id, ok = self.selector.select_server('key01')
        self.assertEqual(22, server_id)
        self.assertEqual(True, ok)

        # select again
        self.selector.reset()
        self.rand_val = 990000

        server_id, ok = self.selector.select_server('key01')
        self.assertEqual(23, server_id)
        self.assertEqual(True, ok)

        # select again
        self.selector.reset()
        self.rand_val = 989999

        server_id, ok = self.selector.select_server('key01')
        self.assertEqual(22, server_id)
        self.assertEqual(True, ok)

    def test_choose_servers_all_zeros(self) -> None:
        self.rand_val = 333333
        self.stats.mem = {
            21: 0.0,
            22: 0.0,
            23: 0.0,
        }

        server_id, ok = self.selector.select_server('key01')
        self.assertEqual(21, server_id)
        self.assertEqual(True, ok)

        # select again
        self.selector.reset()
        self.rand_val = 333334

        server_id, ok = self.selector.select_server('key01')
        self.assertEqual(22, server_id)
        self.assertEqual(True, ok)

    def test_set_failed(self) -> None:
        self.rand_val = 0
        self.stats.mem = {
            21: 100.0,
            22: 100.0,
            23: 100.0,
        }

        self.selector.set_failed_server(21)
        self.assertEqual([21], self.stats.notify_calls)

        server_id, ok = self.selector.select_server('key01')
        self.assertEqual(22, server_id)
        self.assertEqual(True, ok)

        # set again
        self.selector.set_failed_server(21)
        self.assertEqual([21], self.stats.notify_calls)

        # select again
        self.selector.reset()
        self.rand_val = 500000

        server_id, ok = self.selector.select_server('key01')
        self.assertEqual(23, server_id)
        self.assertEqual(True, ok)

        # select again
        self.selector.reset()
        self.rand_val = 499999

        server_id, ok = self.selector.select_server('key01')
        self.assertEqual(22, server_id)
        self.assertEqual(True, ok)

    def test_set_failed_after_select(self) -> None:
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

        self.selector.set_failed_server(21)
        self.assertEqual([21], self.stats.notify_calls)

        server_id, ok = self.selector.select_server('key01')
        self.assertEqual(22, server_id)
        self.assertEqual(True, ok)

        self.assertEqual([21, 22, 23, 22, 23], self.stats.get_calls)

        # set again
        self.selector.set_failed_server(21)
        self.assertEqual([21], self.stats.notify_calls)

        # get again
        server_id, ok = self.selector.select_server('key01')
        self.assertEqual(22, server_id)
        self.assertEqual(True, ok)
        self.assertEqual([21, 22, 23, 22, 23], self.stats.get_calls)

    def test_set_failed_for_all(self) -> None:
        self.stats.mem = {
            21: 100.0,
            22: 100.0,
            23: 100.0,
        }

        self.selector.set_failed_server(21)
        self.selector.set_failed_server(22)
        self.selector.set_failed_server(23)
        self.selector.set_failed_server(21)

        self.assertEqual([21, 22, 23], self.stats.notify_calls)

        self.rand_val = 0

        server_id, ok = self.selector.select_server('key01')
        self.assertEqual(21, server_id)
        self.assertEqual(False, ok)

        # select again
        self.selector.reset()
        self.rand_val = 333334

        server_id, ok = self.selector.select_server('key01')
        self.assertEqual(22, server_id)
        self.assertEqual(False, ok)

        # select again
        self.selector.reset()
        self.rand_val = 666667

        server_id, ok = self.selector.select_server('key01')
        self.assertEqual(23, server_id)
        self.assertEqual(False, ok)

    def test_rand_val_too_big(self) -> None:
        self.rand_val = RAND_MAX + 1
        self.stats.mem = {
            21: 100.0,
            22: 100.0,
            23: 100.0,
        }

        server_id, ok = self.selector.select_server('key01')
        self.assertEqual(23, server_id)
        self.assertEqual(True, ok)

    def test_stats_first_server_failed(self) -> None:
        self.rand_val = 0
        self.stats.mem = {
            21: 100.0,
            22: 100.0,
            23: 100.0,
        }

        self.assertEqual([], self.stats.notify_calls)
        self.stats.failed_servers.add(21)

        server_id, ok = self.selector.select_server('key01')
        self.assertEqual(22, server_id)
        self.assertEqual(True, ok)

    def test_select_for_delete(self) -> None:
        self.stats.mem = {
            21: 100.0,
            22: 100.0,
            23: 100.0,
        }

        servers = self.selector.select_servers_for_delete('key01')
        self.assertEqual([21, 22, 23], servers)

        self.assertEqual([21, 22, 23], self.stats.get_calls)
        self.assertEqual([RAND_MAX], self.rand_calls)

    def test_select_for_delete_after__stats_failed(self) -> None:
        self.stats.mem = {
            21: 100.0,
            22: 100.0,
            23: 100.0,
        }

        self.stats.failed_servers.add(21)

        servers = self.selector.select_servers_for_delete('key01')
        self.assertEqual([22, 23], servers)

        self.assertEqual([21, 22, 23], self.stats.get_calls)
        self.assertEqual([RAND_MAX], self.rand_calls)


class TestReplicatedSelectorFailed(unittest.TestCase):
    def test_empty_server_ids(self):
        self.stats = StatsFake()

        with self.assertRaises(ValueError) as e:
            ReplicatedRoute([], self.stats)

        self.assertEqual(('server_ids must not be empty',), e.exception.args)
