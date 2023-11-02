import unittest

import redis

from memproxy import RedisClient, LeaseGetResponse, LeaseGetStatus


class TestRedisClient(unittest.TestCase):
    def setUp(self):
        self.redis_client = redis.Redis()
        self.redis_client.flushall()
        self.redis_client.script_flush()

    def test_get_single_key(self) -> None:
        c = RedisClient(self.redis_client)
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
        c = RedisClient(self.redis_client)
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
