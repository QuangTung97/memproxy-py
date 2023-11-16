import unittest
from dataclasses import dataclass
from typing import List, Dict, Any
from typing import Optional

import redis

from memproxy import CacheClient, RedisClient
from memproxy import LeaseGetResponse, LeaseSetResponse, DeleteResponse
from memproxy import LeaseSetStatus, DeleteStatus

FOUND = 1
LEASE_GRANTED: int = 2
ERROR = 3


def lease_get_resp(status: int, data: bytes, cas: int, error: Optional[str] = None) -> LeaseGetResponse:
    return status, data, cas, error


class TestRedisClient(unittest.TestCase):
    def setUp(self):
        self.redis_client = redis.Redis()
        self.redis_client.flushall()
        self.redis_client.script_flush()

    def test_get_single_key(self) -> None:
        c: CacheClient = RedisClient(self.redis_client)
        pipe = c.pipeline()

        fn1 = pipe.lease_get('key01')
        resp = fn1.result()

        self.assertEqual(lease_get_resp(
            status=LEASE_GRANTED,
            data=bytes(),
            cas=1,
        ), resp)

        # Get Again
        fn1 = pipe.lease_get('key01')
        resp = fn1.result()

        self.assertEqual(lease_get_resp(
            status=LEASE_GRANTED,
            data=bytes(),
            cas=1,
        ), resp)

    def test_get_multiple_keys(self) -> None:
        c = RedisClient(self.redis_client)
        pipe = c.pipeline()

        fn1 = pipe.lease_get('key01')
        fn2 = pipe.lease_get('key02')

        resp1 = fn1.result()
        resp2 = fn2.result()

        self.assertEqual(lease_get_resp(
            status=LEASE_GRANTED,
            data=b'',
            cas=1,
        ), resp1)

        self.assertEqual(lease_get_resp(
            status=LEASE_GRANTED,
            data=b'',
            cas=2,
        ), resp2)

    def test_get_then_set(self) -> None:
        c = RedisClient(self.redis_client, min_ttl=80, max_ttl=90)
        pipe = c.pipeline()

        fn1 = pipe.lease_get('key01')
        resp = fn1.result()

        set_fn1 = pipe.lease_set('key01', resp[2], b'some-data')
        self.assertEqual(LeaseSetResponse(status=LeaseSetStatus.OK), set_fn1())

        fn2 = pipe.lease_get('key01')
        resp = fn2.result()

        self.assertEqual(lease_get_resp(
            status=FOUND,
            data=b'some-data',
            cas=0,
        ), resp)

        ttl = self.redis_client.ttl('key01')
        self.assertGreater(ttl, 80 - 2)
        self.assertLess(ttl, 90 + 2)

    def test_set_then_get(self) -> None:
        c = RedisClient(self.redis_client)
        pipe = c.pipeline()
        self.addCleanup(pipe.finish)

        set_fn1 = pipe.lease_set('key01', 11, b'some-data')
        self.assertEqual(LeaseSetResponse(status=LeaseSetStatus.NOT_FOUND), set_fn1())

        fn1 = pipe.lease_get('key01')
        resp = fn1.result()

        self.assertEqual(lease_get_resp(
            status=LEASE_GRANTED,
            data=b'',
            cas=1,
        ), resp)

    def test_get_then_set_with_incorrect_cas_value(self) -> None:
        c = RedisClient(self.redis_client)
        pipe = c.pipeline()

        fn1 = pipe.lease_get('key01')
        resp = fn1.result()

        set_fn1 = pipe.lease_set('key01', resp[2] + 1, b'some-data')
        self.assertEqual(LeaseSetResponse(status=LeaseSetStatus.CAS_MISMATCH), set_fn1())

        fn2 = pipe.lease_get('key01')
        resp = fn2.result()

        self.assertEqual(lease_get_resp(
            status=LEASE_GRANTED,
            data=b'',
            cas=1,
        ), resp)

    def test_get_then_delete(self) -> None:
        c = RedisClient(self.redis_client)
        pipe = c.pipeline()
        self.addCleanup(pipe.finish)

        self.redis_client.set('__next_cas', '130')

        fn1 = pipe.lease_get('key01')
        resp = fn1.result()

        self.assertEqual(lease_get_resp(
            status=LEASE_GRANTED,
            data=b'',
            cas=131,
        ), resp)

        delete_fn1 = pipe.delete('key01')
        delete_fn2 = pipe.delete('key02')
        self.assertEqual(DeleteResponse(status=DeleteStatus.OK), delete_fn1())
        self.assertEqual(DeleteResponse(status=DeleteStatus.NOT_FOUND), delete_fn2())

        # Delete Again
        delete_fn1 = pipe.delete('key01')
        self.assertEqual(DeleteResponse(status=DeleteStatus.NOT_FOUND), delete_fn1())

        resp = self.redis_client.get('key01')
        self.assertEqual(None, resp)

    def test_get_delete_then_set(self) -> None:
        c = RedisClient(self.redis_client)
        pipe = c.pipeline()

        fn1 = pipe.lease_get('key01')
        resp = fn1.result()

        delete_fn1 = pipe.delete('key01')
        delete_fn1()

        set_fn1 = pipe.lease_set('key01', resp[2], b'value01')
        self.assertEqual(LeaseSetResponse(status=LeaseSetStatus.NOT_FOUND), set_fn1())

        fn2 = pipe.lease_get('key01')
        resp = fn2.result()

        self.assertEqual(lease_get_resp(
            status=LEASE_GRANTED,
            data=b'',
            cas=2,
        ), resp)

    def test_get_set_then_finish(self) -> None:
        c = RedisClient(self.redis_client)
        pipe = c.pipeline()

        fn1 = pipe.lease_get('key01')
        resp = fn1.result()

        pipe.lease_set('key01', resp[2], b'value01')
        pipe.finish()

        resp = self.redis_client.get('key01')
        self.assertEqual(b'val:value01', resp)

    def test_finish_empty(self) -> None:
        c = RedisClient(self.redis_client)
        pipe = c.pipeline()
        pipe.finish()

    def test_using_with(self) -> None:
        c = RedisClient(self.redis_client)
        with c.pipeline() as pipe:
            fn1 = pipe.lease_get('key01')
            resp = fn1.result()

            pipe.lease_set('key01', resp[2], b'value01')

        resp = self.redis_client.get('key01')
        self.assertEqual(b'val:value01', resp)

    def test_get_then_set_multiple_keys(self) -> None:
        c = RedisClient(self.redis_client)
        pipe = c.pipeline()

        fn1 = pipe.lease_get('key01')
        fn2 = pipe.lease_get('key02')

        resp1 = fn1.result()
        resp2 = fn2.result()

        set_fn1 = pipe.lease_set('key01', resp1[2], b'value01')
        set_fn2 = pipe.lease_set('key02', resp2[2], b'value02')

        set_fn1()
        set_fn2()

        # Get Again
        fn1 = pipe.lease_get('key01')
        fn2 = pipe.lease_get('key02')

        resp1 = fn1.result()
        resp2 = fn2.result()

        self.assertEqual(lease_get_resp(
            status=FOUND,
            data=b'value01',
            cas=0,
        ), resp1)

        self.assertEqual(lease_get_resp(
            status=FOUND,
            data=b'value02',
            cas=0,
        ), resp2)

        ttl = self.redis_client.ttl('key01')
        self.assertGreater(ttl, 3600 * 6 - 2)
        self.assertLess(ttl, 3600 * 12 + 2)

        # Delete Multi Keys
        delete_fn1 = pipe.delete('key01')
        delete_fn2 = pipe.delete('key02')

        delete_fn1()
        delete_fn2()

    def test_get_incorrect_cas(self) -> None:
        c: CacheClient = RedisClient(self.redis_client)
        pipe = c.pipeline()

        self.redis_client.set('key01', b'cas:abc')

        fn1 = pipe.lease_get('key01')
        resp = fn1.result()

        self.assertEqual(lease_get_resp(
            status=ERROR,
            data=b'',
            cas=0,
            error='Value "abc" is not a number',
        ), resp)

    def test_get_without_prefix(self) -> None:
        c: CacheClient = RedisClient(self.redis_client)
        pipe = c.pipeline()

        self.redis_client.set('key01', b'hello01')

        fn1 = pipe.lease_get('key01')
        resp = fn1.result()

        self.assertEqual(lease_get_resp(
            status=FOUND,
            data=b'hello01',
            cas=0,
        ), resp)

    def test_get_lower_session(self) -> None:
        c: CacheClient = RedisClient(self.redis_client)
        pipe = c.pipeline()

        sess = pipe.lower_session()

        calls = []

        sess.add_next_call(lambda: calls.append(21))
        sess.add_next_call(lambda: calls.append(22))

        sess.execute()

        self.assertEqual([21, 22], calls)

    def test_flush_script_in_between(self) -> None:
        c: CacheClient = RedisClient(self.redis_client)
        pipe = c.pipeline()

        self.redis_client.set('__next_cas', 130)

        fn1 = pipe.lease_get('key01')
        resp = fn1.result()

        self.assertEqual(lease_get_resp(
            status=LEASE_GRANTED,
            data=b'',
            cas=131,
        ), resp)

        self.redis_client.script_flush()

        pipe.delete('key01')()

        # Get Again
        fn1 = pipe.lease_get('key01')
        resp = fn1.result()

        self.assertEqual(lease_get_resp(
            status=LEASE_GRANTED,
            data=b'',
            cas=132,
        ), resp)


class TestRedisClientError(unittest.TestCase):
    def setUp(self):
        self.redis_client = redis.Redis(port=6400)

    def test_lease_get(self) -> None:
        c: CacheClient = RedisClient(self.redis_client)
        pipe = c.pipeline()

        fn1 = pipe.lease_get('key01')
        resp = fn1.result()

        self.assertEqual(lease_get_resp(
            status=ERROR,
            data=b'',
            cas=0,
            error='Redis Get: Error 111 connecting to localhost:6400. Connection refused.'
        ), resp)

    def test_lease_set(self) -> None:
        c: CacheClient = RedisClient(self.redis_client)
        pipe = c.pipeline()

        fn1 = pipe.lease_set('key01', 11, b'some-data')
        resp = fn1()

        self.assertEqual(LeaseSetResponse(
            status=LeaseSetStatus.ERROR,
            error='Redis Set: Error 111 connecting to localhost:6400. Connection refused.'
        ), resp)

    def test_delete(self) -> None:
        c: CacheClient = RedisClient(self.redis_client)
        pipe = c.pipeline()

        fn1 = pipe.delete('key01')
        resp = fn1()

        self.assertEqual(DeleteResponse(
            status=DeleteStatus.ERROR,
            error='Redis Delete: Error 111 connecting to localhost:6400. Connection refused.'
        ), resp)


@dataclass
class ScriptCall:
    index: int
    kwargs: Dict


class CapturingRedis(redis.Redis):
    text_list: List[Any]
    script_calls: List[ScriptCall]

    def __init__(self, **kwargs):
        self.text_list = []
        self.script_calls = []
        super().__init__(**kwargs)

    def register_script(self, script: Any) -> Any:
        index = len(self.text_list)
        self.text_list.append(script)

        redis_script = super().register_script(script)

        def script_wrapper(**kwargs):
            self.script_calls.append(ScriptCall(index=index, kwargs=kwargs))
            return redis_script(**kwargs)

        result: Any = script_wrapper
        return result


class TestRedisClientWithCapturing(unittest.TestCase):
    def setUp(self):
        self.redis = CapturingRedis()
        self.redis.flushall()
        self.redis.script_flush()

    def test_get_and_set_single_key(self) -> None:
        c: CacheClient = RedisClient(self.redis, min_ttl=70, max_ttl=70)
        pipe = c.pipeline()

        fn1 = pipe.lease_get('key01')
        resp1 = fn1.result()
        self.assertEqual(lease_get_resp(data=b'', cas=1, status=LEASE_GRANTED), resp1)

        set_fn1 = pipe.lease_set('key01', resp1[2], b'value01')
        self.assertEqual(LeaseSetResponse(status=LeaseSetStatus.OK), set_fn1())

        self.assertEqual(2, len(self.redis.text_list))

        calls = self.redis.script_calls
        self.assertEqual(2, len(calls))

        self.assertEqual(0, calls[0].index)
        self.assertDictEqual({
            'client': self.redis,
            'keys': ['key01'],
        }, calls[0].kwargs)

        self.assertEqual(1, calls[1].index)
        self.assertDictEqual({
            'client': self.redis,
            'keys': ['key01'],
            'args': [
                1, b'value01', 70,
            ],
        }, calls[1].kwargs)

    def test_get_multi_keys__exceed_max_batch(self) -> None:
        c: CacheClient = RedisClient(self.redis, max_keys_per_batch=3)
        pipe = c.pipeline()

        fn1 = pipe.lease_get('key01')
        fn2 = pipe.lease_get('key02')
        fn3 = pipe.lease_get('key03')
        fn4 = pipe.lease_get('key04')

        self.assertEqual(lease_get_resp(data=b'', cas=1, status=LEASE_GRANTED), fn1.result())
        self.assertEqual(lease_get_resp(data=b'', cas=2, status=LEASE_GRANTED), fn2.result())
        self.assertEqual(lease_get_resp(data=b'', cas=3, status=LEASE_GRANTED), fn3.result())
        self.assertEqual(lease_get_resp(data=b'', cas=4, status=LEASE_GRANTED), fn4.result())

        self.assertEqual(2, len(self.redis.text_list))

        calls = self.redis.script_calls
        self.assertEqual(2, len(calls))

        # Check Call 1
        self.assertEqual(0, calls[0].index)

        del calls[0].kwargs['client']
        self.assertDictEqual({
            'keys': ['key01', 'key02', 'key03'],
        }, calls[0].kwargs)

        # Check Call 2
        self.assertEqual(0, calls[1].index)

        del calls[1].kwargs['client']
        self.assertDictEqual({
            'keys': ['key04'],
        }, calls[1].kwargs)

        # Check Near Exceed
        delete_fn1 = pipe.delete('key01')
        delete_fn2 = pipe.delete('key02')
        delete_fn3 = pipe.delete('key03')

        delete_fn1()
        delete_fn2()
        delete_fn3()

        fn1 = pipe.lease_get('key01')
        fn2 = pipe.lease_get('key02')
        fn3 = pipe.lease_get('key03')

        self.assertEqual(lease_get_resp(data=b'', cas=5, status=LEASE_GRANTED), fn1.result())
        self.assertEqual(lease_get_resp(data=b'', cas=6, status=LEASE_GRANTED), fn2.result())
        self.assertEqual(lease_get_resp(data=b'', cas=7, status=LEASE_GRANTED), fn3.result())

        self.assertEqual(3, len(calls))

        # Check Call 3
        self.assertEqual(0, calls[2].index)

        del calls[2].kwargs['client']
        self.assertDictEqual({
            'keys': ['key01', 'key02', 'key03'],
        }, calls[2].kwargs)

    def test_get_and_set_multi_keys__exceed_max_batch(self) -> None:
        c: CacheClient = RedisClient(self.redis, max_keys_per_batch=3, min_ttl=70, max_ttl=70)
        pipe = c.pipeline()

        fn1 = pipe.lease_get('key01')
        fn2 = pipe.lease_get('key02')
        fn3 = pipe.lease_get('key03')
        fn4 = pipe.lease_get('key04')

        resp1 = fn1.result()
        resp2 = fn2.result()
        resp3 = fn3.result()
        resp4 = fn4.result()

        set_fn1 = pipe.lease_set('key01', resp1[2], b'value01')
        pipe.lease_set('key02', resp2[2], b'value02')
        pipe.lease_set('key03', resp3[2], b'value03')
        pipe.lease_set('key04', resp4[2], b'value04')

        set_resp = set_fn1()
        self.assertEqual(LeaseSetResponse(LeaseSetStatus.OK), set_resp)

        calls = self.redis.script_calls
        self.assertEqual(4, len(calls))

        call3 = calls[2]
        self.assertEqual(1, call3.index)
        del call3.kwargs['client']
        self.assertDictEqual({
            'keys': ['key01', 'key02', 'key03'],
            'args': [
                1, b'value01', 70,
                2, b'value02', 70,
                3, b'value03', 70,
            ]
        }, call3.kwargs)

        call4 = calls[3]
        self.assertEqual(1, call4.index)
        del call4.kwargs['client']
        self.assertDictEqual({
            'keys': ['key04'],
            'args': [
                4, b'value04', 70,
            ]
        }, call4.kwargs)

        fn1 = pipe.lease_get('key01')
        fn4 = pipe.lease_get('key04')

        self.assertEqual(lease_get_resp(cas=0, data=b'value01', status=FOUND), fn1.result())
        self.assertEqual(lease_get_resp(cas=0, data=b'value04', status=FOUND), fn4.result())
