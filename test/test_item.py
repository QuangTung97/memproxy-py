import cProfile
import datetime
import os
import unittest
from dataclasses import dataclass
from typing import List

import redis

from memproxy import Item, RedisClient, Promise, new_json_codec, ItemCodec
from memproxy import Pipeline, Session, DeleteResponse, LeaseSetResponse, LeaseGetResponse


@dataclass
class UserTest:
    id: int
    name: str
    age: int


@dataclass
class SetInput:
    key: str
    cas: int
    data: bytes


class CapturedPipeline(Pipeline):
    pipe: Pipeline
    get_keys: List[str]
    set_inputs: List[SetInput]

    def __init__(self, pipe: Pipeline):
        self.pipe = pipe
        self.get_keys = []
        self.set_inputs = []

    def lease_get(self, key: str) -> Promise[LeaseGetResponse]:
        self.get_keys.append(key)
        return self.pipe.lease_get(key)

    def lease_set(self, key: str, cas: int, data: bytes) -> Promise[LeaseSetResponse]:
        self.set_inputs.append(SetInput(key, cas, data))
        return self.pipe.lease_set(key, cas, data)

    def delete(self, key: str) -> Promise[DeleteResponse]:
        return self.pipe.delete(key)

    def lower_session(self) -> Session:
        return self.pipe.lower_session()

    def finish(self) -> None:
        return self.pipe.finish()

    def __enter__(self):
        self.pipe.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pipe.__exit__(exc_type, exc_val, exc_tb)


class TestItemIntegration(unittest.TestCase):
    fill_keys: List[int]
    age: int
    pipe: CapturedPipeline

    def setUp(self) -> None:
        self.redis_client = redis.Redis()
        self.redis_client.flushall()
        self.redis_client.script_flush()

        c = RedisClient(self.redis_client)
        self.pipe = CapturedPipeline(c.pipeline())
        self.addCleanup(self.pipe.finish)

        self.fill_keys = []

        self.it = Item[UserTest, int](
            pipe=self.pipe,
            key_fn=lambda user_id: f'user:{user_id}',
            filler=self.filler_func,
            codec=new_json_codec(UserTest),
        )
        self.age = 81

    def filler_func(self, key: int) -> Promise[UserTest]:
        self.fill_keys.append(key)
        return lambda: UserTest(id=key, name=f'user-data:{key}', age=self.age)

    def test_normal(self) -> None:
        it = self.it

        user_fn = it.get(21)
        u = user_fn()

        self.assertEqual(UserTest(id=21, name='user-data:21', age=81), u)

        resp = self.redis_client.get('user:21')
        self.assertEqual(b'val:{"id": 21, "name": "user-data:21", "age": 81}', resp)

        self.assertEqual([21], self.fill_keys)

        # Get Again
        user_fn = it.get(21)
        u = user_fn()
        self.assertEqual(UserTest(id=21, name='user-data:21', age=81), u)

        self.assertEqual('user:23', it.compute_key_name(23))
        self.assertEqual([21], self.fill_keys)

    def test_get_multi(self) -> None:
        it = self.it

        user_fn1 = it.get(21)
        user_fn2 = it.get(22)
        user_fn3 = it.get(23)

        self.assertEqual(UserTest(id=21, name='user-data:21', age=81), user_fn1())
        self.assertEqual(UserTest(id=22, name='user-data:22', age=81), user_fn2())
        self.assertEqual(UserTest(id=23, name='user-data:23', age=81), user_fn3())

        self.assertEqual([21, 22, 23], self.fill_keys)
        self.assertEqual(['user:21', 'user:22', 'user:23'], self.pipe.get_keys)
        self.assertListEqual([
            SetInput('user:21', 1, b'{"id": 21, "name": "user-data:21", "age": 81}'),
            SetInput('user:22', 2, b'{"id": 22, "name": "user-data:22", "age": 81}'),
            SetInput('user:23', 3, b'{"id": 23, "name": "user-data:23", "age": 81}'),
        ], self.pipe.set_inputs)

        # Get Again
        user_fn1 = it.get(21)
        user_fn2 = it.get(22)
        user_fn3 = it.get(23)

        self.assertEqual(UserTest(id=21, name='user-data:21', age=81), user_fn1())
        self.assertEqual(UserTest(id=22, name='user-data:22', age=81), user_fn2())
        self.assertEqual(UserTest(id=23, name='user-data:23', age=81), user_fn3())

        self.assertEqual([21, 22, 23], self.fill_keys)
        self.assertEqual(['user:21', 'user:22', 'user:23'] * 2, self.pipe.get_keys)

        # Do Delete
        delete_fn1 = self.pipe.delete(it.compute_key_name(21))
        delete_fn2 = self.pipe.delete(it.compute_key_name(22))
        delete_fn3 = self.pipe.delete(it.compute_key_name(23))

        delete_fn1()
        delete_fn2()
        delete_fn3()

        self.age = 91

        # Get Again after delete
        user_fn1 = it.get(21)
        user_fn2 = it.get(22)
        user_fn3 = it.get(23)

        self.assertEqual(UserTest(id=21, name='user-data:21', age=91), user_fn1())
        self.assertEqual(UserTest(id=22, name='user-data:22', age=91), user_fn2())
        self.assertEqual(UserTest(id=23, name='user-data:23', age=91), user_fn3())

        self.assertEqual([21, 22, 23, 21, 22, 23], self.fill_keys)

    def test_get_decode_error(self) -> None:
        codec: ItemCodec[UserTest] = new_json_codec(UserTest)

        def decode_fn(data: bytes) -> UserTest:
            raise ValueError('can not decode')

        codec.decode = decode_fn

        it = Item[UserTest, int](
            pipe=self.pipe,
            key_fn=lambda user_id: f'user:{user_id}',
            filler=self.filler_func,
            codec=codec,
        )

        user_fn = it.get(21)
        self.assertEqual(UserTest(id=21, name='user-data:21', age=81), user_fn())

        self.assertEqual(1, len(self.pipe.set_inputs))
        self.assertEqual('user:21', self.pipe.set_inputs[0].key)

        # Get Again
        user_fn = it.get(21)
        self.assertEqual(UserTest(id=21, name='user-data:21', age=81), user_fn())


class TestItemRedisError(unittest.TestCase):
    fill_keys: List[int]
    age: int
    pipe: CapturedPipeline

    def setUp(self) -> None:
        self.redis_client = redis.Redis(port=6400)
        c = RedisClient(self.redis_client)

        self.pipe = CapturedPipeline(c.pipeline())

        self.fill_keys = []

        self.it = Item[UserTest, int](
            pipe=self.pipe,
            key_fn=lambda user_id: f'user:{user_id}',
            filler=self.filler_func,
            codec=new_json_codec(UserTest),
        )
        self.age = 81

    def filler_func(self, key: int) -> Promise[UserTest]:
        self.fill_keys.append(key)
        return lambda: UserTest(id=key, name=f'user-data:{key}', age=self.age)

    def test_single_key(self) -> None:
        it = self.it

        user_fn = it.get(21)
        u = user_fn()

        self.assertEqual(UserTest(id=21, name='user-data:21', age=81), u)

        self.assertEqual([21], self.fill_keys)
        self.assertEqual([], self.pipe.set_inputs)


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
