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
