import unittest
from typing import Dict

from memproxy import CacheClient
from memproxy import LeaseGetResponse, LeaseGetStatus
from memproxy.proxy import ProxyCacheClient, ReplicatedRoute
from .fake_pipe import ClientFake, global_get_calls
from .fake_stats import StatsFake


class TestProxy(unittest.TestCase):
    clients: Dict[int, ClientFake]
    rand_val: int

    def setUp(self) -> None:
        global_get_calls.clear()

        self.server_ids = [21, 22, 23]
        self.stats = StatsFake()
        self.clients = {}

        self.rand_val = 0

        self.stats.mem = {
            21: 100,
            22: 100,
            23: 100,
        }

        self.route = ReplicatedRoute(self.server_ids, self.stats, rand=self.rand_func)
        self.client: CacheClient = ProxyCacheClient(self.server_ids, self.new_func, self.route)

        self.pipe = self.client.pipeline()

    def rand_func(self, _: int):
        return self.rand_val

    def new_func(self, server_id) -> CacheClient:
        c = ClientFake()
        self.clients[server_id] = c
        return c

    def test_lease_get(self) -> None:
        fn = self.pipe.lease_get('key01')

        self.assertEqual(3, len(self.clients))
        self.assertIn(21, self.clients)
        self.assertIn(23, self.clients)

        calls = self.clients[21].new_calls
        self.assertEqual(1, len(calls))

        assert calls[0] is not None
        self.assertIs(self.pipe.lower_session(), calls[0].get_lower().get_lower())

        pipe1 = self.clients[21].pipe
        resp1 = LeaseGetResponse(
            status=LeaseGetStatus.FOUND,
            cas=0,
            data=b'data 01',
        )
        pipe1.get_results = [resp1]

        result = fn()
        self.assertEqual(resp1, result)

        self.assertEqual(['key01', 'key01:func'], pipe1.get_calls)

    def test_lease_get_multi(self) -> None:
        fn1 = self.pipe.lease_get('key01')
        fn2 = self.pipe.lease_get('key02')
        fn3 = self.pipe.lease_get('key03')

        resp1 = LeaseGetResponse(
            status=LeaseGetStatus.FOUND,
            cas=0,
            data=b'data 01',
        )
        resp2 = LeaseGetResponse(
            status=LeaseGetStatus.LEASE_GRANTED,
            cas=51,
            data=b'',
        )
        resp3 = LeaseGetResponse(
            status=LeaseGetStatus.FOUND,
            cas=0,
            data=b'data 03',
        )

        pipe1 = self.clients[21].pipe
        pipe1.get_results = [resp1, resp2, resp3]

        self.assertEqual(resp1, fn1())
        self.assertEqual(resp2, fn2())
        self.assertEqual(resp3, fn3())

        self.assertEqual([
            'key01', 'key02', 'key03',
            'key01:func', 'key02:func', 'key03:func',
        ], pipe1.get_calls)

    def test_lease_get_error_retry_on_another(self) -> None:
        fn = self.pipe.lease_get('key01')

        resp1 = LeaseGetResponse(
            status=LeaseGetStatus.ERROR,
            cas=0,
            data=b'',
            error='server error'
        )
        resp2 = LeaseGetResponse(
            status=LeaseGetStatus.FOUND,
            cas=0,
            data=b'data 01',
        )

        pipe1 = self.clients[21].pipe
        pipe1.get_results = [resp1]

        pipe2 = self.clients[22].pipe
        pipe2.get_results = [resp2]

        self.assertEqual(resp2, fn())

        self.assertEqual(['key01', 'key01:func'], pipe1.get_calls)
        self.assertEqual(['key01', 'key01:func'], pipe2.get_calls)
        self.assertEqual(['key01', 'key01:func', 'key01', 'key01:func'], global_get_calls)
