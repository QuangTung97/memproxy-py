import unittest
from dataclasses import dataclass

from memproxy.pool import ObjectPool


@dataclass
class UserTest:
    id: int
    name: str


def new_user() -> UserTest:
    return UserTest(id=0, name='')


class TestObjectPool(unittest.TestCase):
    def setUp(self) -> None:
        self.pool = ObjectPool(
            new_func=new_user,
            max_size=3,
        )

    def test_normal(self) -> None:
        u = self.pool.get()
        self.assertEqual(UserTest(id=0, name=''), u)

        u = self.pool.get()
        self.assertEqual(UserTest(id=0, name=''), u)

        self.assertEqual(0, self.pool.pool_size())

        self.pool.put(UserTest(id=3, name='abc'))
        self.pool.put(UserTest(id=4, name=''))
        self.pool.put(UserTest(id=5, name='x'))
        self.pool.put(UserTest(id=0, name=''))

        self.assertEqual(3, self.pool.pool_size())

        u = self.pool.get()
        self.assertEqual(UserTest(id=5, name='x'), u)

        self.assertEqual(2, self.pool.pool_size())
