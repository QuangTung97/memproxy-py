import time
import unittest
from dataclasses import dataclass
from typing import List, Optional

import redis

from memproxy import DeleteResponse, DeleteStatus
from memproxy import Item, ItemCodec, LeaseGetResponse, new_json_codec
from memproxy import RedisClient, Promise, CacheClient
from memproxy.proxy import ProxyCacheClient, ReplicatedRoute
from .fake_pipe import ClientFake
from .fake_stats import StatsFake

FOUND = 1
LEASE_GRANTED: int = 2
ERROR = 3


def lease_get_resp(status: int, data: bytes, cas: int, error: Optional[str] = None) -> LeaseGetResponse:
    return status, data, cas, error


@dataclass
class UserTest:
    id: int
    name: str

    def encode(self) -> bytes:
        return self.name.encode()


def user_key_name(user_id: int) -> str:
    return f'users:{user_id}'


def decode_user(data: bytes) -> UserTest:
    return UserTest(id=0, name=data.decode())


user_codec = ItemCodec(
    encode=UserTest.encode,
    decode=decode_user,
)


class TestProxyItemBenchmark(unittest.TestCase):
    fill_keys: List[int]

    def setUp(self) -> None:
        self.redis = redis.Redis()
        self.redis.flushall()
        self.redis.script_flush()

        self.servers = [21]
        self.fill_keys = []

        self.stats = StatsFake()
        self.stats.mem = {
            21: 1000,
        }

        route = ReplicatedRoute(
            server_ids=self.servers,
            stats=self.stats,
        )

        self.proxy_client = ProxyCacheClient(
            server_ids=self.servers,
            new_func=self.new_func,
            route=route,
        )

    def new_func(self, _server_id: int) -> CacheClient:
        return RedisClient(self.redis, max_keys_per_batch=200)

    def filler_func(self, key: int) -> Promise[UserTest]:
        self.fill_keys.append(key)
        return lambda: UserTest(id=key, name='A' * 100)

    def run_multi_get(self) -> None:
        pipe = self.proxy_client.pipeline()

        it = Item(
            pipe=pipe,
            key_fn=user_key_name,
            codec=user_codec,
            filler=self.filler_func,
        )

        keys = list(range(100))
        fn = it.get_multi(keys)
        fn()

    def test_run_benchmark_proxy(self) -> None:
        self.run_multi_get()

        start = time.perf_counter_ns()

        num_loops = 100
        for i in range(num_loops):
            self.run_multi_get()

        duration = time.perf_counter_ns() - start

        print(f'AVG PROXY ITEM DURATION: {(duration / num_loops) / 1000.0}us')


NUM_KEYS = 100


class TestProxyItemBenchmarkInMemory(unittest.TestCase):
    client: ClientFake

    def setUp(self) -> None:
        self.servers = [21]

        self.stats = StatsFake()
        self.stats.mem = {
            21: 1000,
        }

        self.client = ClientFake()
        resp = lease_get_resp(status=FOUND, cas=0, data=b'user name 01')
        self.client.pipe.get_results = [resp] * NUM_KEYS

        route = ReplicatedRoute(
            server_ids=self.servers,
            stats=self.stats,
        )

        self.proxy_client = ProxyCacheClient(
            server_ids=self.servers,
            new_func=self.new_client,
            route=route,
        )

    def new_client(self, _server_id: int) -> CacheClient:
        return self.client

    def filler_func(self, key: int) -> Promise[UserTest]:
        raise NotImplementedError("some error")

    def run_multi_get(self) -> None:
        pipe = self.proxy_client.pipeline()

        self.client.pipe.get_keys = []

        it = Item(
            pipe=pipe,
            key_fn=user_key_name,
            codec=user_codec,
            filler=self.filler_func,
        )

        keys = list(range(NUM_KEYS))
        fn = it.get_multi(keys)
        _users = fn()

    def test_run_benchmark_proxy(self) -> None:
        num_loops = 20000  # 405.9151384 us => 382.4579183us

        start = time.perf_counter_ns()

        for i in range(num_loops):
            self.run_multi_get()

        duration = time.perf_counter_ns() - start
        print(f'[MEMORY ONLY] AVG PROXY ITEM DURATION: {duration / 1000 / num_loops}us')

    def test_run_do_init_only(self) -> None:
        num_loops = 40000

        start = time.perf_counter_ns()

        count = 0

        for i in range(num_loops):
            pipe = self.proxy_client.pipeline()

            it = Item(
                pipe=pipe,
                key_fn=user_key_name,
                codec=user_codec,
                filler=self.filler_func,
            )
            k = it.compute_key_name(10)
            count += len(k)

        duration = time.perf_counter_ns() - start
        print(f'[MEMORY ONLY] DO INIT ONLY AVG DURATION: {duration / num_loops / 1000}us')


class TestProxyItem(unittest.TestCase):
    fill_keys: List[int]
    prefix: str

    def setUp(self) -> None:
        self.redis = redis.Redis()
        self.redis.flushall()
        self.redis.script_flush()

        self.servers = [21]
        self.fill_keys = []
        self.prefix = 'username'

        self.stats = StatsFake()
        self.stats.mem = {
            21: 1000,
        }

        route = ReplicatedRoute(
            server_ids=self.servers,
            stats=self.stats,
        )

        self.proxy_client = ProxyCacheClient(
            server_ids=self.servers,
            new_func=self.new_func,
            route=route,
        )

        self.pipe = self.proxy_client.pipeline()

        self.it: Item[UserTest, int] = Item(
            pipe=self.pipe,
            key_fn=user_key_name,
            codec=new_json_codec(UserTest),
            filler=self.filler_func,
        )

    def new_func(self, _server_id: int) -> CacheClient:
        return RedisClient(self.redis, max_keys_per_batch=200)

    def filler_func(self, key: int) -> Promise[UserTest]:
        self.fill_keys.append(key)
        return lambda: UserTest(id=key, name=f'{self.prefix}:{key}')

    def test_normal(self) -> None:
        fn = self.it.get_multi([11, 12, 13])
        users = fn()

        self.assertEqual([
            UserTest(id=11, name='username:11'),
            UserTest(id=12, name='username:12'),
            UserTest(id=13, name='username:13'),
        ], users)

        self.assertEqual([11, 12, 13], self.fill_keys)

        # get again
        self.prefix = 'newuser'
        users = self.it.get_multi([11, 12])()

        self.assertEqual([
            UserTest(id=11, name='username:11'),
            UserTest(id=12, name='username:12'),
        ], users)

        self.assertEqual([11, 12, 13], self.fill_keys)

        # do delete
        delete_fn1 = self.pipe.delete(self.it.compute_key_name(11))
        delete_fn2 = self.pipe.delete(self.it.compute_key_name(12))

        self.assertEqual(DeleteResponse(status=DeleteStatus.OK), delete_fn1())
        self.assertEqual(DeleteResponse(status=DeleteStatus.OK), delete_fn2())

        # get again after delete
        fn = self.it.get_multi([11, 12, 13])
        users = fn()

        self.assertEqual([
            UserTest(id=11, name='newuser:11'),
            UserTest(id=12, name='newuser:12'),
            UserTest(id=13, name='username:13'),
        ], users)

        self.assertEqual([11, 12, 13, 11, 12], self.fill_keys)

    def test_with(self):
        with self.proxy_client.pipeline() as pipe:
            it = Item(
                pipe=pipe,
                key_fn=user_key_name,
                codec=new_json_codec(UserTest),
                filler=self.filler_func,
            )

            fn = it.get_multi([11, 12, 13])
            users = fn()

            self.assertEqual([
                UserTest(id=11, name='username:11'),
                UserTest(id=12, name='username:12'),
                UserTest(id=13, name='username:13'),
            ], users)

            self.assertEqual([11, 12, 13], self.fill_keys)
