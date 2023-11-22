from __future__ import annotations

import cProfile
import datetime
import os
import unittest
from dataclasses import dataclass
from typing import List, Union

import redis

from memproxy import Item, RedisClient, Promise, new_json_codec, ItemCodec, new_multi_get_filler
from memproxy import LeaseGetResult
from memproxy import Pipeline, Session, DeleteResponse, LeaseSetResponse, LeaseGetResponse, FillerFunc
from memproxy.memproxy import LeaseGetResultFunc


@dataclass
class UserTest:
    id: int
    name: str
    age: int

    @staticmethod
    def get_key(u: UserTest) -> int:
        return u.id


@dataclass
class SetInput:
    key: str
    cas: int
    data: bytes


class CapturedPipeline(Pipeline):
    pipe: Pipeline
    get_keys: List[str]
    set_inputs: List[Union[SetInput, str]]

    def __init__(self, pipe: Pipeline):
        self.pipe = pipe
        self.get_keys = []
        self.set_inputs = []

    def lease_get(self, key: str) -> LeaseGetResult:
        self.get_keys.append(key)
        fn = self.pipe.lease_get(key)

        def lease_get_func() -> LeaseGetResponse:
            self.get_keys.append(f'{key}:func')
            return fn.result()

        return LeaseGetResultFunc(lease_get_func)

    def lease_set(self, key: str, cas: int, data: bytes) -> Promise[LeaseSetResponse]:
        self.set_inputs.append(SetInput(key, cas, data))
        fn = self.pipe.lease_set(key, cas, data)

        def lease_set_func() -> LeaseSetResponse:
            self.set_inputs.append(f'{key}:func')
            return fn()

        return lease_set_func

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
        self.assertEqual(0, it.bytes_read)

        resp = self.redis_client.get('user:21')
        self.assertEqual(b'val:{"id": 21, "name": "user-data:21", "age": 81}', resp)

        self.assertEqual([21], self.fill_keys)

        self.assertEqual(0, it.hit_count)
        self.assertEqual(1, it.fill_count)

        # Get Again
        user_fn = it.get(21)
        u = user_fn()
        self.assertEqual(UserTest(id=21, name='user-data:21', age=81), u)

        self.assertEqual('user:23', it.compute_key_name(23))
        self.assertEqual([21], self.fill_keys)

        self.assertEqual(1, it.hit_count)
        self.assertEqual(1, it.fill_count)
        self.assertEqual(len(b'{"id": 21, "name": "user-data:21", "age": 81}'), it.bytes_read)

        # Get Again on more time
        user_fn = it.get(21)
        u = user_fn()
        self.assertEqual(UserTest(id=21, name='user-data:21', age=81), u)

        self.assertEqual('user:23', it.compute_key_name(23))
        self.assertEqual([21], self.fill_keys)

        self.assertEqual(2, it.hit_count)
        self.assertEqual(1, it.fill_count)
        self.assertEqual(0, it.cache_error_count)
        self.assertEqual(2 * len(b'{"id": 21, "name": "user-data:21", "age": 81}'), it.bytes_read)

        # Delete
        self.pipe.delete(it.compute_key_name(21))()

        # Get Again after Delete
        user_fn = it.get(21)
        u = user_fn()
        self.assertEqual(UserTest(id=21, name='user-data:21', age=81), u)

        self.assertEqual(2, it.hit_count)
        self.assertEqual(2, it.fill_count)
        self.assertEqual(0, it.cache_error_count)
        self.assertEqual(2 * len(b'{"id": 21, "name": "user-data:21", "age": 81}'), it.bytes_read)

    def test_get_multi(self) -> None:
        it = self.it

        user_fn1 = it.get(21)
        user_fn2 = it.get(22)
        user_fn3 = it.get(23)

        self.assertEqual(UserTest(id=21, name='user-data:21', age=81), user_fn1())
        self.assertEqual(UserTest(id=22, name='user-data:22', age=81), user_fn2())
        self.assertEqual(UserTest(id=23, name='user-data:23', age=81), user_fn3())

        self.assertEqual([21, 22, 23], self.fill_keys)
        self.assertEqual([
            'user:21', 'user:22', 'user:23',
            'user:21:func', 'user:22:func', 'user:23:func',
        ], self.pipe.get_keys)
        self.assertListEqual([
            SetInput('user:21', 1, b'{"id": 21, "name": "user-data:21", "age": 81}'),
            SetInput('user:22', 2, b'{"id": 22, "name": "user-data:22", "age": 81}'),
            SetInput('user:23', 3, b'{"id": 23, "name": "user-data:23", "age": 81}'),
            'user:21:func',
            'user:22:func',
            'user:23:func',
        ], self.pipe.set_inputs)

        self.assertEqual(0, it.hit_count)
        self.assertEqual(3, it.fill_count)

        # Get Again
        user_fn1 = it.get(21)
        user_fn2 = it.get(22)
        user_fn3 = it.get(23)

        self.assertEqual(UserTest(id=21, name='user-data:21', age=81), user_fn1())
        self.assertEqual(UserTest(id=22, name='user-data:22', age=81), user_fn2())
        self.assertEqual(UserTest(id=23, name='user-data:23', age=81), user_fn3())

        self.assertEqual([21, 22, 23], self.fill_keys)
        self.assertEqual(
            [
                'user:21', 'user:22', 'user:23',
                'user:21:func', 'user:22:func', 'user:23:func',
            ] * 2,
            self.pipe.get_keys)

        self.assertEqual(3, it.hit_count)
        self.assertEqual(3, it.fill_count)

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

        self.assertEqual(3, it.hit_count)
        self.assertEqual(6, it.fill_count)

    def test_get_decode_error(self) -> None:
        codec: ItemCodec[UserTest] = new_json_codec(UserTest)

        def decode_fn(_: bytes) -> UserTest:
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

        self.assertEqual(2, len(self.pipe.set_inputs))

        set_input = self.pipe.set_inputs[0]
        self.assertTrue(isinstance(set_input, SetInput))
        if isinstance(set_input, SetInput):
            self.assertEqual('user:21', set_input.key)
            self.assertEqual(1, set_input.cas)

        self.assertEqual('user:21:func', self.pipe.set_inputs[1])

        # Get Again
        user_fn = it.get(21)
        self.assertEqual(UserTest(id=21, name='user-data:21', age=81), user_fn())

        self.assertEqual(2, len(self.pipe.set_inputs))

        self.assertEqual(1, it.hit_count)
        self.assertEqual(2, it.fill_count)
        self.assertEqual(0, it.cache_error_count)
        self.assertEqual(1, it.decode_error_count)

    def test_reuse_error(self) -> None:
        it = self.it

        user_fn1 = it.get(21)
        self.assertEqual(UserTest(id=21, name='user-data:21', age=81), user_fn1())

        # Get with different key
        user_fn2 = it.get(22)
        self.assertEqual(UserTest(id=22, name='user-data:22', age=81), user_fn2())

        self.assertEqual(UserTest(id=21, name='user-data:21', age=81), user_fn1())


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

        self.assertEqual(0, it.hit_count)
        self.assertEqual(1, it.fill_count)
        self.assertEqual(1, it.cache_error_count)


class TestItemBenchmark(unittest.TestCase):
    fill_keys: List[int]

    def setUp(self) -> None:
        self.redis_client = redis.Redis()
        self.redis_client.flushall()
        self.redis_client.script_flush()

        c = RedisClient(self.redis_client, max_keys_per_batch=200)
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

        num_loops = 100
        for i in range(num_loops):
            self.run_multi_get()
        print(f'AVG DURATION: {(self.total_duration / num_loops).microseconds / 1000.0}ms')

    def test_run_with_profiler(self) -> None:
        if not os.getenv("ENABLE_PROFILE"):
            self.skipTest('Default not run')

        pr = cProfile.Profile()
        pr.enable()

        num_loops = 2000
        for i in range(num_loops):
            self.run_multi_get()

        print("AVG DURATION:", (self.total_duration / num_loops).microseconds / 1000.0)
        pr.dump_stats('item.stats')

        pr.disable()


class TestMultiGetFiller(unittest.TestCase):
    fill_keys: List[List[int]]
    return_users: List[UserTest]
    default: UserTest

    def setUp(self) -> None:
        self.fill_keys = []
        self.default = UserTest(id=0, name='', age=0)

    def new_filler(self) -> FillerFunc[int, UserTest]:
        return new_multi_get_filler(
            fill_func=self.fill_func,
            get_key_func=UserTest.get_key,
            default=self.default,
        )

    def fill_func(self, keys: List[int]) -> List[UserTest]:
        self.fill_keys.append(keys)
        return self.return_users

    def test_simple(self) -> None:
        f = self.new_filler()

        u1 = UserTest(id=21, name='user01', age=71)
        u2 = UserTest(id=22, name='user02', age=72)

        self.return_users = [u1, u2]

        fn1 = f(21)
        fn2 = f(22)
        fn3 = f(23)

        self.assertEqual([], self.fill_keys)

        self.assertEqual(u1, fn1())
        self.assertEqual(u2, fn2())
        self.assertEqual(UserTest(id=0, name='', age=0), fn3())

        self.assertEqual([[21, 22, 23]], self.fill_keys)

    def test_single(self) -> None:
        f = self.new_filler()

        self.return_users = []

        fn1 = f(21)

        self.assertEqual(UserTest(id=0, name='', age=0), fn1())

        self.assertEqual([[21]], self.fill_keys)

    def test_call_multiple_phases(self) -> None:
        f = self.new_filler()

        u1 = UserTest(id=21, name='user01', age=71)
        u2 = UserTest(id=22, name='user02', age=72)

        self.return_users = [u1, u2]

        fn1 = f(21)
        fn2 = f(22)
        fn3 = f(23)

        self.assertEqual(u1, fn1())
        self.assertEqual(u2, fn2())
        self.assertEqual(UserTest(id=0, name='', age=0), fn3())

        # call again
        u3 = UserTest(id=23, name='user03', age=73)
        u4 = UserTest(id=21, name='user12', age=82)

        self.return_users = [u4, u2, u3]
        fn1 = f(21)
        fn3 = f(23)

        self.assertEqual(u4, fn1())
        self.assertEqual(u3, fn3())

        self.assertEqual([
            [21, 22, 23],
            [21, 23],
        ], self.fill_keys)

    def test_interleave(self) -> None:
        f = self.new_filler()

        u1 = UserTest(id=21, name='user01', age=71)
        u2 = UserTest(id=22, name='user02', age=72)
        u3 = UserTest(id=23, name='user03', age=73)
        u4 = UserTest(id=24, name='user04', age=74)

        self.return_users = [u1, u2]

        fn1 = f(21)
        fn2 = f(22)

        self.assertEqual(u1, fn1())

        self.return_users = [u3, u4]

        fn3 = f(23)
        fn4 = f(24)

        self.assertEqual(u2, fn2())

        self.assertEqual(u3, fn3())
        self.assertEqual(u4, fn4())

        self.assertEqual([
            [21, 22],
            [23, 24],
        ], self.fill_keys)


class TestItemGetMulti(unittest.TestCase):
    fill_keys: List[List[int]]
    age: int
    pipe: CapturedPipeline

    def setUp(self) -> None:
        self.redis = redis.Redis()
        self.redis.flushall()
        self.redis.script_flush()

        c = RedisClient(self.redis)
        self.pipe = CapturedPipeline(c.pipeline())

        self.fill_keys = []

        self.it = Item[UserTest, int](
            pipe=self.pipe,
            key_fn=lambda user_id: f'user:{user_id}',
            filler=new_multi_get_filler(
                fill_func=self.fill_multi,
                get_key_func=UserTest.get_key,
                default=UserTest(id=0, name='', age=0),
            ),
            codec=new_json_codec(UserTest),
        )
        self.age = 81

    def fill_multi(self, keys: List[int]) -> List[UserTest]:
        self.fill_keys.append(keys)
        return [UserTest(id=k, name=f'user:{k}', age=self.age) for k in keys]

    def test_normal(self) -> None:
        it = self.it

        fn1 = it.get_multi([21, 22, 23])
        fn2 = it.get_multi([24, 25])

        self.assertEqual([
            UserTest(id=21, name='user:21', age=81),
            UserTest(id=22, name='user:22', age=81),
            UserTest(id=23, name='user:23', age=81),
        ], fn1())

        self.assertEqual([
            UserTest(id=24, name='user:24', age=81),
            UserTest(id=25, name='user:25', age=81),
        ], fn2())

        self.assertEqual([[21, 22, 23, 24, 25]], self.fill_keys)

        self.assertEqual([
            'user:21', 'user:22', 'user:23',
            'user:24', 'user:25',
            'user:21:func', 'user:22:func', 'user:23:func',
            'user:24:func', 'user:25:func',
        ], self.pipe.get_keys)
