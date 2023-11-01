import unittest
from typing import List

from memproxy import Session


class TestSample(unittest.TestCase):
    def test_hello(self) -> None:
        self.assertEqual('3', '3')

    def test_tung(self) -> None:
        self.assertEqual(5, 5)


class TestSession(unittest.TestCase):
    def test_simple(self) -> None:
        calls = []
        sess = Session()

        sess.add_next_call(lambda: calls.append('A'))
        sess.add_next_call(lambda: calls.append('B'))
        sess.add_next_call(lambda: calls.append('C'))

        sess.execute()

        self.assertEqual(['A', 'B', 'C'], calls)

        calls = []

        sess.add_next_call(lambda: calls.append('E'))
        sess.add_next_call(lambda: calls.append('F'))
        sess.execute()

        self.assertEqual(['E', 'F'], calls)

    def test_add_next_call_inside(self) -> None:
        sess = Session()

        calls: List[int] = []

        def handler_02():
            calls.append(12)

        def handler_01():
            sess.add_next_call(handler_02)
            calls.append(11)

        sess.add_next_call(handler_01)

        sess.execute()

        self.assertEqual([11, 12], calls)
