import unittest
from dataclasses import dataclass

from memproxy.pool import ObjectPool


@dataclass
class UserTest:
    id: int
    name: str


class TestObjectPool(unittest.TestCase):
    def setUp(self) -> None:
        self.pool = ObjectPool[UserTest](
            clazz=UserTest,
            max_size=3,
        )

    def test_normal(self) -> None:
        u = self.pool.get(id=11, name='name01')
        self.assertEqual(UserTest(id=11, name='name01'), u)

        u = self.pool.get(id=12, name='name02')
        self.assertEqual(UserTest(id=12, name='name02'), u)

        self.assertEqual(0, self.pool.pool_size())

        self.pool.put(UserTest(id=3, name='abc'))
        self.pool.put(UserTest(id=4, name=''))
        self.pool.put(UserTest(id=5, name='x'))
        self.pool.put(UserTest(id=99, name=''))

        self.assertEqual(3, self.pool.pool_size())

        u = self.pool.get(id=6, name='y')
        self.assertEqual(UserTest(id=6, name='y'), u)

        self.assertEqual(2, self.pool.pool_size())
