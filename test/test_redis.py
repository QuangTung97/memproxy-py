import unittest

import redis

from memproxy import CacheClient, RedisClient, LeaseGetResponse, LeaseGetStatus, LeaseSetResponse, DeleteResponse


class TestRedisClient(unittest.TestCase):
    def setUp(self):
        self.redis_client = redis.Redis()
        self.redis_client.flushall()
        self.redis_client.script_flush()

    def test_get_single_key(self) -> None:
        c: CacheClient = RedisClient(self.redis_client)
        pipe = c.pipeline()

        fn1 = pipe.lease_get('key01')
        resp = fn1()

        self.assertEqual(LeaseGetResponse(
            status=LeaseGetStatus.LEASE_GRANTED,
            data=bytes(),
            cas=1,
        ), resp)

        # Get Again
        fn1 = pipe.lease_get('key01')
        resp = fn1()

        self.assertEqual(LeaseGetResponse(
            status=LeaseGetStatus.LEASE_GRANTED,
            data=bytes(),
            cas=1,
        ), resp)

    def test_get_multiple_keys(self) -> None:
        c = RedisClient(self.redis_client)
        pipe = c.pipeline()

        fn1 = pipe.lease_get('key01')
        fn2 = pipe.lease_get('key02')

        resp1 = fn1()
        resp2 = fn2()

        self.assertEqual(LeaseGetResponse(
            status=LeaseGetStatus.LEASE_GRANTED,
            data=b'',
            cas=1,
        ), resp1)

        self.assertEqual(LeaseGetResponse(
            status=LeaseGetStatus.LEASE_GRANTED,
            data=b'',
            cas=2,
        ), resp2)

    def test_get_then_set(self) -> None:
        c = RedisClient(self.redis_client, min_ttl=80, max_ttl=90)
        pipe = c.pipeline()

        fn1 = pipe.lease_get('key01')
        resp = fn1()

        set_fn1 = pipe.lease_set('key01', resp.cas, b'some-data')
        set_fn1()

        fn2 = pipe.lease_get('key01')
        resp = fn2()

        self.assertEqual(LeaseGetResponse(
            status=LeaseGetStatus.FOUND,
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
        set_fn1()

        fn1 = pipe.lease_get('key01')
        resp = fn1()

        self.assertEqual(LeaseGetResponse(
            status=LeaseGetStatus.LEASE_GRANTED,
            data=b'',
            cas=1,
        ), resp)

    def test_get_then_set_with_incorrect_cas_value(self) -> None:
        c = RedisClient(self.redis_client)
        pipe = c.pipeline()

        fn1 = pipe.lease_get('key01')
        resp = fn1()

        set_fn1 = pipe.lease_set('key01', resp.cas + 1, b'some-data')
        set_fn1()

        fn2 = pipe.lease_get('key01')
        resp = fn2()

        self.assertEqual(LeaseGetResponse(
            status=LeaseGetStatus.LEASE_GRANTED,
            data=b'',
            cas=1,
        ), resp)

    def test_get_then_delete(self) -> None:
        c = RedisClient(self.redis_client)
        pipe = c.pipeline()
        self.addCleanup(pipe.finish)

        self.redis_client.set('__next_cas', '130')

        fn1 = pipe.lease_get('key01')
        resp = fn1()

        self.assertEqual(LeaseGetResponse(
            status=LeaseGetStatus.LEASE_GRANTED,
            data=b'',
            cas=131,
        ), resp)

        delete_fn1 = pipe.delete('key01')
        delete_fn1()

        resp = self.redis_client.get('key01')
        self.assertEqual(None, resp)

    def test_get_delete_then_set(self) -> None:
        c = RedisClient(self.redis_client)
        pipe = c.pipeline()

        fn1 = pipe.lease_get('key01')
        resp = fn1()

        delete_fn1 = pipe.delete('key01')
        delete_fn1()

        set_fn1 = pipe.lease_set('key01', resp.cas, b'value01')
        set_fn1()

        fn2 = pipe.lease_get('key01')
        resp = fn2()

        self.assertEqual(LeaseGetResponse(
            status=LeaseGetStatus.LEASE_GRANTED,
            data=b'',
            cas=2,
        ), resp)

    def test_get_set_then_finish(self) -> None:
        c = RedisClient(self.redis_client)
        pipe = c.pipeline()

        fn1 = pipe.lease_get('key01')
        resp = fn1()

        pipe.lease_set('key01', resp.cas, b'value01')
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
            resp = fn1()

            pipe.lease_set('key01', resp.cas, b'value01')

        resp = self.redis_client.get('key01')
        self.assertEqual(b'val:value01', resp)

    def test_get_then_set_multiple_keys(self) -> None:
        c = RedisClient(self.redis_client)
        pipe = c.pipeline()

        fn1 = pipe.lease_get('key01')
        fn2 = pipe.lease_get('key02')

        resp1 = fn1()
        resp2 = fn2()

        set_fn1 = pipe.lease_set('key01', resp1.cas, b'value01')
        set_fn2 = pipe.lease_set('key02', resp2.cas, b'value02')

        set_fn1()
        set_fn2()

        # Get Again
        fn1 = pipe.lease_get('key01')
        fn2 = pipe.lease_get('key02')

        resp1 = fn1()
        resp2 = fn2()

        self.assertEqual(LeaseGetResponse(
            status=LeaseGetStatus.FOUND,
            data=b'value01',
            cas=0,
        ), resp1)

        self.assertEqual(LeaseGetResponse(
            status=LeaseGetStatus.FOUND,
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
        resp = fn1()

        self.assertEqual(LeaseGetResponse(
            status=LeaseGetStatus.ERROR,
            data=b'',
            cas=0,
            error='Value "abc" is not a number',
        ), resp)

    def test_get_without_prefix(self) -> None:
        c: CacheClient = RedisClient(self.redis_client)
        pipe = c.pipeline()

        self.redis_client.set('key01', b'hello01')

        fn1 = pipe.lease_get('key01')
        resp = fn1()

        self.assertEqual(LeaseGetResponse(
            status=LeaseGetStatus.FOUND,
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
        resp = fn1()

        self.assertEqual(LeaseGetResponse(
            status=LeaseGetStatus.LEASE_GRANTED,
            data=b'',
            cas=131,
        ), resp)

        self.redis_client.script_flush()

        pipe.delete('key01')()

        # Get Again
        fn1 = pipe.lease_get('key01')
        resp = fn1()

        self.assertEqual(LeaseGetResponse(
            status=LeaseGetStatus.LEASE_GRANTED,
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
        resp = fn1()

        self.assertEqual(LeaseGetResponse(
            status=LeaseGetStatus.ERROR,
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
            error='Redis Set: Error 111 connecting to localhost:6400. Connection refused.'
        ), resp)

    def test_delete(self) -> None:
        c: CacheClient = RedisClient(self.redis_client)
        pipe = c.pipeline()

        fn1 = pipe.delete('key01')
        resp = fn1()

        self.assertEqual(DeleteResponse(
            error='Redis Delete: Error 111 connecting to localhost:6400. Connection refused.'
        ), resp)
