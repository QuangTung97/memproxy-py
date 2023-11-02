import unittest
from dataclasses import dataclass
from typing import List

import redis

from memproxy import Item, RedisClient, Promise, new_json_codec


@dataclass
class UserTest:
    id: int
    name: str
    age: int


class TestItemIntegration(unittest.TestCase):
    fill_keys: List[int]

    def setUp(self) -> None:
        self.redis_client = redis.Redis()
        self.redis_client.flushall()
        self.redis_client.script_flush()

        c = RedisClient(self.redis_client)
        self.pipe = c.pipeline()
        self.addCleanup(self.pipe.finish)

        self.fill_keys = []

    def filler_func(self, key: int) -> Promise[UserTest]:
        self.fill_keys.append(key)
        return lambda: UserTest(id=21, name='user01', age=81)

    def test_normal(self) -> None:
        it = Item[UserTest, int](
            pipe=self.pipe,
            key_fn=lambda user_id: f'user:{user_id}',
            filler=self.filler_func,
            codec=new_json_codec(UserTest),
        )

        user_fn = it.get(21)
        u = user_fn()

        self.assertEqual(UserTest(id=21, name='user01', age=81), u)

        resp = self.redis_client.get('user:21')
        self.assertEqual(b'val:{"id": 21, "name": "user01", "age": 81}', resp)

        # Get Again
        user_fn = it.get(21)
        u = user_fn()
        self.assertEqual(UserTest(id=21, name='user01', age=81), u)

        self.assertEqual('user:23', it.compute_key_name(23))
