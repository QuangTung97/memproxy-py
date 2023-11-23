import random
import unittest
from dataclasses import dataclass
from typing import Dict, List

import redis

from memproxy import RedisClient, Item, new_multi_get_filler, new_json_codec, DeleteResponse, DeleteStatus, Promise
from memproxy.proxy import ReplicatedRoute, ServerStats, ProxyCacheClient


@dataclass
class UserTest:
    user_id: int
    username: str
    counter: int

    def get_id(self) -> int:
        return self.user_id


@dataclass
class RoleTest:
    role_id: int
    role_name: str

    def get_role_id(self) -> int:
        return self.role_id


class TestProxyPropertyBased(unittest.TestCase):
    user_dict: Dict[int, UserTest]
    role_dict: Dict[int, RoleTest]

    def setUp(self) -> None:
        self.user_dict = {}
        self.role_dict = {}

        self.client1 = redis.Redis()
        self.client1.flushall()
        self.client1.script_flush()

        self.client2 = redis.Redis(port=6380)
        self.client2.flushall()
        self.client2.script_flush()

        clients = {
            21: self.client1,
            22: self.client2,
        }
        server_ids = [21, 22]

        stats = ServerStats(clients=clients)

        route = ReplicatedRoute(server_ids=server_ids, stats=stats)

        def new_redis_client(server_id: int):
            return RedisClient(r=clients[server_id])

        self.cache = ProxyCacheClient(
            server_ids=server_ids, new_func=new_redis_client,
            route=route,
        )

    def get_users(self, user_ids: List[int]) -> List[UserTest]:
        result = []
        for user_id in user_ids:
            if user_id in self.user_dict:
                result.append(self.user_dict[user_id])
        return result

    def reset_pipe(self):
        self.pipe = self.cache.pipeline()

    def new_user_item(self) -> Item[UserTest, int]:
        filler = new_multi_get_filler(
            fill_func=self.get_users,
            get_key_func=UserTest.get_id,
            default=UserTest(user_id=0, username='', counter=0),
        )

        return Item(
            pipe=self.pipe,
            key_fn=lambda user_id: f'users:{user_id}',
            filler=filler,
            codec=new_json_codec(UserTest),
        )

    def new_error_user_item(self) -> Item[UserTest, int]:
        def filler(user_id: int) -> Promise[UserTest]:
            raise ValueError('can not get user')

        return Item(
            pipe=self.pipe,
            key_fn=lambda user_id: f'users:{user_id}',
            filler=filler,
            codec=new_json_codec(UserTest),
        )

    def test_normal(self) -> None:
        random.seed(113)

        for i in range(100):
            self.reset_pipe()
            user_item = self.new_user_item()

            users = user_item.get_multi([11, 12])()
            self.assertEqual([
                UserTest(user_id=0, username='', counter=0),
                UserTest(user_id=0, username='', counter=0),
            ], users)

        self.assertEqual(b'val:{"user_id": 0, "username": "", "counter": 0}', self.client1.get('users:11'))
        self.assertEqual(b'val:{"user_id": 0, "username": "", "counter": 0}', self.client2.get('users:11'))
        self.assertEqual(b'val:{"user_id": 0, "username": "", "counter": 0}', self.client1.get('users:12'))

        # Update User Dict
        self.user_dict[11] = UserTest(user_id=11, username='username 01', counter=41)

        # Get Again Same Value
        for i in range(100):
            self.reset_pipe()
            user_item = self.new_user_item()

            users = user_item.get_multi([11, 12])()
            self.assertEqual([
                UserTest(user_id=0, username='', counter=0),
                UserTest(user_id=0, username='', counter=0),
            ], users)

        # Do Deletion
        self.reset_pipe()
        user_item = self.new_user_item()
        fn1 = self.pipe.delete(user_item.compute_key_name(11))
        fn2 = self.pipe.delete(user_item.compute_key_name(12))

        self.assertEqual(DeleteResponse(status=DeleteStatus.OK), fn1())
        self.assertEqual(DeleteResponse(status=DeleteStatus.OK), fn2())

        self.assertEqual(None, self.client1.get('users:11'))
        self.assertEqual(None, self.client2.get('users:11'))
        self.assertEqual(None, self.client1.get('users:12'))

        # Get Again with New Value
        for i in range(100):
            self.reset_pipe()
            user_item = self.new_user_item()

            users = user_item.get_multi([11, 12])()
            self.assertEqual([
                UserTest(user_id=11, username='username 01', counter=41),
                UserTest(user_id=0, username='', counter=0),
            ], users)

        self.assertEqual(
            b'val:{"user_id": 11, "username": "username 01", "counter": 41}',
            self.client1.get('users:11'),
        )
        self.assertEqual(
            b'val:{"user_id": 11, "username": "username 01", "counter": 41}',
            self.client2.get('users:11'),
        )
        self.assertEqual(b'val:{"user_id": 0, "username": "", "counter": 0}', self.client1.get('users:12'))

        # Update And Delete
        self.user_dict[11].counter += 2
        self.reset_pipe()
        user_item = self.new_user_item()
        fn1 = self.pipe.delete(user_item.compute_key_name(11))
        fn2 = self.pipe.delete(user_item.compute_key_name(12))
        fn1()
        fn2()

        # Get Again with New Counter
        for i in range(100):
            self.reset_pipe()
            user_item = self.new_user_item()

            users = user_item.get_multi([11])()
            self.assertEqual([
                UserTest(user_id=11, username='username 01', counter=43),
            ], users)

    def test_get_error(self) -> None:
        self.reset_pipe()
        error_item = self.new_error_user_item()
        fn1 = error_item.get(61)
        fn2 = error_item.get_multi([62, 63])

        with self.assertRaises(ValueError) as ex:
            fn1()
            fn2()

        self.assertEqual(('can not get user',), ex.exception.args)

        self.user_dict[11] = UserTest(user_id=11, username='user 11', counter=81)
        self.user_dict[12] = UserTest(user_id=12, username='user 12', counter=82)

        for i in range(100):
            self.reset_pipe()
            user_item = self.new_user_item()

            users = user_item.get_multi([11, 12])()
            self.assertEqual([
                UserTest(user_id=11, username='user 11', counter=81),
                UserTest(user_id=12, username='user 12', counter=82),
            ], users)

        self.assertEqual(
            b'val:{"user_id": 11, "username": "user 11", "counter": 81}',
            self.client1.get('users:11'),
        )
        self.assertEqual(
            b'val:{"user_id": 11, "username": "user 11", "counter": 81}',
            self.client2.get('users:11'),
        )
        self.assertEqual(
            b'val:{"user_id": 12, "username": "user 12", "counter": 82}',
            self.client1.get('users:12'),
        )
