import unittest
from typing import Dict

from memproxy import CacheClient, DeleteResponse, DeleteStatus
from memproxy import LeaseGetResponse, LeaseGetStatus
from memproxy import LeaseSetResponse, LeaseSetStatus
from memproxy.proxy import ProxyCacheClient, ReplicatedRoute
from .fake_pipe import ClientFake, global_actions, SetInput
from .fake_stats import StatsFake


class TestProxy(unittest.TestCase):
    clients: Dict[int, ClientFake]
    rand_val: int

    def setUp(self) -> None:
        global_actions.clear()

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

        result = fn.result()
        self.assertEqual(resp1, result)

        self.assertEqual(['key01', 'key01:func'], pipe1.actions)

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

        self.assertEqual(resp1, fn1.result())
        self.assertEqual(resp2, fn2.result())
        self.assertEqual(resp3, fn3.result())

        self.assertEqual([
            'key01', 'key02', 'key03',
            'key01:func', 'key02:func', 'key03:func',
        ], pipe1.actions)

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

        self.assertEqual(resp2, fn.result())

        self.assertEqual(['key01', 'key01:func'], pipe1.actions)
        self.assertEqual(['key01', 'key01:func'], pipe2.actions)
        self.assertEqual(['key01', 'key01:func', 'key01', 'key01:func'], global_actions)

    def test_lease_get_retry_server_already_failed(self) -> None:
        self.stats.failed_servers.add(21)
        self.stats.failed_servers.add(22)

        fn = self.pipe.lease_get('key01')

        resp1 = LeaseGetResponse(
            status=LeaseGetStatus.ERROR,
            cas=0,
            data=b'',
            error='server error'
        )

        pipe1 = self.clients[23].pipe
        pipe1.get_results = [resp1]

        self.assertEqual(resp1, fn.result())

        self.assertEqual(['key01', 'key01:func'], pipe1.actions)

    def test_lease_get_then_set(self) -> None:
        resp1 = LeaseGetResponse(
            status=LeaseGetStatus.LEASE_GRANTED,
            cas=61,
            data=b'',
        )

        pipe1 = self.clients[21].pipe
        pipe1.get_results = [resp1]

        fn1 = self.pipe.lease_get('key01')
        self.assertEqual(resp1, fn1.result())

        set_fn1 = self.pipe.lease_set('key01', resp1.cas, b'data 01')
        self.assertEqual(LeaseSetResponse(LeaseSetStatus.OK), set_fn1())

        self.assertEqual([
            SetInput(key='key01', cas=resp1.cas, val=b'data 01')
        ], pipe1.set_calls)

        self.assertEqual([
            'key01', 'key01:func', 'set key01', 'set key01:func'
        ], pipe1.actions)

    def test_lease_set_only(self) -> None:
        pipe1 = self.clients[21].pipe

        set_fn1 = self.pipe.lease_set('key01', 71, b'data 01')
        self.assertEqual(LeaseSetResponse(LeaseSetStatus.ERROR, error='proxy: can not do lease set'), set_fn1())

        self.assertEqual([], pipe1.set_calls)
        self.assertEqual([], pipe1.actions)

    def test_lease_get_then_set_to_another_server(self) -> None:
        resp1 = LeaseGetResponse(
            status=LeaseGetStatus.LEASE_GRANTED,
            cas=61,
            data=b'',
        )
        resp2 = LeaseGetResponse(
            status=LeaseGetStatus.ERROR,
            cas=0,
            data=b'',
        )

        pipe1 = self.clients[21].pipe
        pipe1.get_results = [resp1, resp2]

        fn1 = self.pipe.lease_get('key01')
        self.assertEqual(resp1, fn1.result())

        # get again switch to another server
        resp3 = LeaseGetResponse(
            status=LeaseGetStatus.LEASE_GRANTED,
            cas=71,
            data=b'',
        )

        pipe2 = self.clients[22].pipe
        pipe2.get_results = [resp3]

        fn2 = self.pipe.lease_get('key01')
        self.assertEqual(resp3, fn2.result())

        # lease set should fail
        set_fn1 = self.pipe.lease_set('key01', resp1.cas, b'data 01')
        self.assertEqual(LeaseSetResponse(LeaseSetStatus.ERROR, error='proxy: can not do lease set'), set_fn1())

        self.assertEqual([], pipe1.set_calls)

        self.assertEqual([
            'key01', 'key01:func', 'key01', 'key01:func'
        ], pipe1.actions)

        self.assertEqual([
            'key01', 'key01:func', 'key01', 'key01:func',
            'key01', 'key01:func'
        ], global_actions)

    def test_delete(self) -> None:
        fn1 = self.pipe.delete('key01')
        fn2 = self.pipe.delete('key02')

        self.assertEqual(DeleteResponse(status=DeleteStatus.OK), fn1())
        self.assertEqual(DeleteResponse(status=DeleteStatus.OK), fn2())

        pipe1 = self.clients[21].pipe
        pipe2 = self.clients[22].pipe
        pipe3 = self.clients[23].pipe

        self.assertEqual([
            'del key01', 'del key02', 'del key01:func', 'del key02:func'
        ], pipe1.actions)
        self.assertEqual([
            'del key01', 'del key02', 'del key01:func', 'del key02:func'
        ], pipe2.actions)
        self.assertEqual([
            'del key01', 'del key02', 'del key01:func', 'del key02:func'
        ], pipe3.actions)

        self.assertEqual([
            'del key01', 'del key01', 'del key01',
            'del key02', 'del key02', 'del key02',
            'del key01:func', 'del key01:func', 'del key01:func',
            'del key02:func', 'del key02:func', 'del key02:func',
        ], global_actions)

    def test_delete_with_failed_server(self) -> None:
        self.stats.failed_servers.add(21)

        fn1 = self.pipe.delete('key01')

        self.assertEqual(DeleteResponse(status=DeleteStatus.OK), fn1())

        pipe2 = self.clients[22].pipe
        pipe3 = self.clients[23].pipe

        self.assertEqual([
            'del key01', 'del key01:func',
        ], pipe2.actions)
        self.assertEqual([
            'del key01', 'del key01:func',
        ], pipe3.actions)

        self.assertEqual([
            'del key01', 'del key01',
            'del key01:func', 'del key01:func',
        ], global_actions)

    def test_delete_with_no_servers(self) -> None:
        self.stats.failed_servers.add(21)
        self.stats.failed_servers.add(22)
        self.stats.failed_servers.add(23)

        fn1 = self.pipe.delete('key01')

        self.assertEqual(DeleteResponse(status=DeleteStatus.NOT_FOUND), fn1())

        self.assertEqual([], global_actions)

    def test_finish(self) -> None:
        self.pipe.delete('key01')

        self.pipe.finish()

        pipe1 = self.clients[21].pipe
        pipe2 = self.clients[22].pipe
        pipe3 = self.clients[23].pipe

        self.assertEqual([
            'del key01',
            'finish',
        ], pipe1.actions)
        self.assertEqual([
            'del key01',
            'finish',
        ], pipe2.actions)
        self.assertEqual([
            'del key01',
            'finish',
        ], pipe3.actions)

        self.assertEqual([
            'del key01', 'del key01', 'del key01',
            'finish', 'finish', 'finish'
        ], global_actions)
