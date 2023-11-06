import unittest
from typing import Dict

from memproxy import CacheClient
from memproxy.proxy import ProxyCacheClient, ReplicatedRoute
from .fake_pipe import ClientFake
from .fake_stats import StatsFake


class TestProxy(unittest.TestCase):
    clients: Dict[int, ClientFake]

    def setUp(self) -> None:
        self.server_ids = [21, 22, 23]
        self.stats = StatsFake()
        self.clients = {}

        self.stats.mem = {
            21: 100,
            22: 100,
            23: 100,
        }

        self.route = ReplicatedRoute(self.server_ids, self.stats)
        self.client: CacheClient = ProxyCacheClient(self.server_ids, self.new_func, self.route)

        self.pipe = self.client.pipeline()

    def new_func(self, server_id) -> CacheClient:
        c = ClientFake()
        self.clients[server_id] = c
        return c

    def test_normal(self) -> None:
        self.pipe.lease_get('key01')

        self.assertEqual(3, len(self.clients))
        self.assertIn(21, self.clients)
        self.assertIn(23, self.clients)
