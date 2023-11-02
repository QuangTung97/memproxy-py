import cProfile
import datetime
import os
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


class TestItemBenchmark(unittest.TestCase):
    fill_keys: List[int]

    def setUp(self) -> None:
        self.redis_client = redis.Redis()
        self.redis_client.flushall()
        self.redis_client.script_flush()

        c = RedisClient(self.redis_client)
        self.pipe = c.pipeline()
        self.addCleanup(self.pipe.finish)

        self.fill_keys = []
        self.total_duration = datetime.timedelta(0)

    def filler_func(self, key: int) -> Promise[UserTest]:
        self.fill_keys.append(key)
        return lambda: UserTest(id=21, name='user01', age=81)

    def run_multi_get(self) -> None:
        start = datetime.datetime.now()

        it = Item[UserTest, int](
            pipe=self.pipe,
            key_fn=lambda user_id: f'user:{user_id}',
            filler=self.filler_func,
            codec=new_json_codec(UserTest),
        )

        fn_list: List[Promise[UserTest]] = []
        for i in range(100):
            fn = it.get(i)
            fn_list.append(fn)

        for fn in fn_list:
            fn()

        duration = datetime.datetime.now() - start
        self.total_duration += duration

    def test_run(self) -> None:
        self.run_multi_get()
        self.total_duration = datetime.timedelta(0)

        num_loops = 10
        for i in range(num_loops):
            self.run_multi_get()
        print(f'AVG DURATION: {(self.total_duration / num_loops).microseconds / 1000.0}ms')

    def test_run_with_profiler(self) -> None:
        if not os.getenv("ENABLE_PROFILE"):
            self.skipTest('Default not run')

        with cProfile.Profile() as pr:
            num_loops = 1000
            for i in range(num_loops):
                self.run_multi_get()

            print("AVG DURATION:", (self.total_duration / num_loops).microseconds / 1000.0)
            pr.dump_stats('item.stats')
