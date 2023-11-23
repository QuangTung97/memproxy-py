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


@dataclass(frozen=True)
class RoleKey:
    tenant: str
    code: str


@dataclass
class RoleTest:
    tenant: str
    code: str
    name: str

    def get_key(self) -> RoleKey:
        return RoleKey(tenant=self.tenant, code=self.code)


class TestProxyPropertyBased(unittest.TestCase):
    user_dict: Dict[int, UserTest]
    role_dict: Dict[RoleKey, RoleTest]

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
        def filler(_user_id: int) -> Promise[UserTest]:
            raise ValueError('can not get user')

        return Item(
            pipe=self.pipe,
            key_fn=lambda user_id: f'users:{user_id}',
            filler=filler,
            codec=new_json_codec(UserTest),
        )

    def get_roles(self, keys: List[RoleKey]) -> List[RoleTest]:
        result = []
        for k in keys:
            if k in self.role_dict:
                result.append(self.role_dict[k])
        return result

    def new_role_item(self) -> Item[RoleTest, RoleKey]:
        def key_fn(k: RoleKey) -> str:
            return f'roles:{k.tenant}:{k.code}'

        filler = new_multi_get_filler(
            fill_func=self.get_roles,
            get_key_func=RoleTest.get_key,
            default=RoleTest(tenant='', code='', name=''),
        )

        return Item(
            pipe=self.pipe,
            key_fn=key_fn,
            filler=filler,
            codec=new_json_codec(RoleTest),
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

    def test_get_both_user_and_role(self) -> None:
        random.seed(113)

        key1 = RoleKey(tenant='TENANT01', code='CODE01')
        key2 = RoleKey(tenant='TENANT02', code='CODE02')

        for i in range(100):
            self.reset_pipe()
            user_item = self.new_user_item()
            role_item = self.new_role_item()

            fn1 = user_item.get(11)
            fn2 = user_item.get(12)
            fn = role_item.get_multi([key1, key2])

            self.assertEqual(UserTest(user_id=0, username='', counter=0), fn1())
            self.assertEqual(UserTest(user_id=0, username='', counter=0), fn2())
            self.assertEqual([
                RoleTest(tenant='', code='', name=''),
                RoleTest(tenant='', code='', name=''),
            ], fn())

        user_data = b'val:{"user_id": 0, "username": "", "counter": 0}'
        self.assertEqual(user_data, self.client1.get('users:11'))
        self.assertEqual(user_data, self.client2.get('users:11'))

        user_data = b'val:{"tenant": "", "code": "", "name": ""}'
        self.assertEqual(user_data, self.client1.get('roles:TENANT01:CODE01'))
        self.assertEqual(user_data, self.client2.get('roles:TENANT01:CODE01'))

        # Update And Delete
        self.user_dict[11] = UserTest(user_id=11, username='user11', counter=51)
        self.role_dict[key1] = RoleTest(
            tenant='TENANT01', code='CODE01', name='Role Name 01',
        )
        self.role_dict[key2] = RoleTest(
            tenant='TENANT02', code='CODE02', name='Role Name 02',
        )

        self.reset_pipe()

        user_item = self.new_user_item()
        role_item = self.new_role_item()

        fn1 = self.pipe.delete(user_item.compute_key_name(11))
        fn2 = self.pipe.delete(role_item.compute_key_name(key1))
        fn3 = self.pipe.delete(role_item.compute_key_name(key2))

        self.assertEqual(DeleteResponse(status=DeleteStatus.OK), fn1())
        self.assertEqual(DeleteResponse(status=DeleteStatus.OK), fn2())
        self.assertEqual(DeleteResponse(status=DeleteStatus.OK), fn3())

        self.assertEqual(None, self.client1.get('roles:TENANT01:CODE01'))
        self.assertEqual(None, self.client2.get('roles:TENANT01:CODE01'))

        # Get Again
        for i in range(100):
            self.reset_pipe()
            user_item = self.new_user_item()
            role_item = self.new_role_item()

            fn1 = user_item.get(11)
            fn2 = user_item.get(12)
            fn = role_item.get_multi([key1, key2])

            self.assertEqual(UserTest(user_id=11, username='user11', counter=51), fn1())
            self.assertEqual(UserTest(user_id=0, username='', counter=0), fn2())
            self.assertEqual([
                RoleTest(tenant='TENANT01', code='CODE01', name='Role Name 01'),
                RoleTest(tenant='TENANT02', code='CODE02', name='Role Name 02'),
            ], fn())

    def test_get_role(self) -> None:
        random.seed(113)

        key1 = RoleKey(tenant='TENANT01', code='CODE01')
        key2 = RoleKey(tenant='TENANT02', code='CODE02')
        key3 = RoleKey(tenant='TENANT01', code='CODE02')

        self.role_dict[key1] = RoleTest(
            tenant='TENANT01', code='CODE01', name='Role Name 01',
        )
        self.role_dict[key2] = RoleTest(
            tenant='TENANT02', code='CODE02', name='Role Name 02',
        )
        self.role_dict[key3] = RoleTest(
            tenant='TENANT01', code='CODE02', name='Role Name 03',
        )

        for i in range(100):
            self.reset_pipe()
            role_item = self.new_role_item()

            fn1 = role_item.get_multi([key1, key2])
            fn2 = role_item.get(key3)

            self.assertEqual([
                RoleTest(tenant='TENANT01', code='CODE01', name='Role Name 01'),
                RoleTest(tenant='TENANT02', code='CODE02', name='Role Name 02'),
            ], fn1())

            self.assertEqual(RoleTest(tenant='TENANT01', code='CODE02', name='Role Name 03'), fn2())
