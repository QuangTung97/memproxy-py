import datetime
import unittest
from dataclasses import dataclass
from typing import List

import redis

from memproxy import Item, ItemCodec
from memproxy import RedisClient, Promise, CacheClient
from memproxy.proxy import ProxyCacheClient, ReplicatedRoute
from .fake_stats import StatsFake


@dataclass
class UserTest:
    id: int
    name: str

    def get_id(self) -> int:
        return self.id

    def encode(self) -> bytes:
        return self.name.encode()


def user_key_name(user_id: int) -> str:
    return f'users:{user_id}'


def decode_user(data: bytes) -> UserTest:
    return UserTest(id=0, name=data.decode())


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

        self.total_duration = datetime.timedelta(0)

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
        start = datetime.datetime.now()

        pipe = self.proxy_client.pipeline()

        it = Item(
            pipe=pipe,
            key_fn=user_key_name,
            codec=ItemCodec(
                encode=UserTest.encode,
                decode=decode_user,
            ),
            filler=self.filler_func,
        )

        fn_list: List[Promise[UserTest]] = []
        for i in range(100):
            fn = it.get(i)
            fn_list.append(fn)

        for fn in fn_list:
            fn()

        duration = datetime.datetime.now() - start
        self.total_duration += duration

    def test_run_benchmark_proxy(self) -> None:
        self.run_multi_get()
        self.total_duration = datetime.timedelta(0)

        num_loops = 10
        for i in range(num_loops):
            self.run_multi_get()

        print(f'AVG PROXY ITEM DURATION: {(self.total_duration / num_loops).microseconds / 1000.0}ms')
