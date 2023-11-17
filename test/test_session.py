import unittest
from typing import List

from memproxy import Session


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

    def test_lower_session(self) -> None:
        sess = Session()
        lower = sess.get_lower()

        self.assertTrue(lower == sess.get_lower())
        self.assertTrue(sess != sess.get_lower())

        calls: List[int] = []

        lower.add_next_call(lambda: calls.append(31))
        lower.add_next_call(lambda: calls.append(32))

        sess.add_next_call(lambda: calls.append(21))
        sess.add_next_call(lambda: calls.append(22))

        lower.execute()

        self.assertEqual([21, 22, 31, 32], calls)

    def test_lower_session__call_multiple_times_is_same(self) -> None:
        sess = Session()
        lower = sess.get_lower()

        self.assertIs(lower, sess.get_lower())
        self.assertIsNot(sess, sess.get_lower())
        self.assertIsNot(lower, sess)

    def test_multi_levels(self) -> None:
        sess = Session()
        lower = sess.get_lower()
        lower2 = lower.get_lower()

        calls: List[int] = []

        lower2.add_next_call(lambda: calls.append(41))

        lower.add_next_call(lambda: calls.append(31))
        lower.add_next_call(lambda: calls.append(32))

        sess.add_next_call(lambda: calls.append(21))
        sess.add_next_call(lambda: calls.append(22))

        lower2.execute()

        self.assertEqual([21, 22, 31, 32, 41], calls)

    def test_execute_at_middle(self) -> None:
        sess = Session()
        lower = sess.get_lower()
        lower2 = lower.get_lower()

        calls: List[int] = []

        lower2.add_next_call(lambda: calls.append(41))
        lower2.add_next_call(lambda: calls.append(42))

        lower.add_next_call(lambda: calls.append(31))
        lower.add_next_call(lambda: calls.append(32))

        sess.add_next_call(lambda: calls.append(21))
        sess.add_next_call(lambda: calls.append(22))

        lower.execute()

        self.assertEqual([21, 22, 31, 32], calls)

        calls = []
        lower.execute()
        self.assertEqual([], calls)

        calls = []
        lower2.execute()
        self.assertEqual([41, 42], calls)

    def test_multi_levels__add_inside(self) -> None:
        sess = Session()
        lower = sess.get_lower()
        lower2 = lower.get_lower()

        calls: List[int] = []

        def lower2_fn():
            calls.append(41)
            lower2.add_next_call(lambda: calls.append(42))

        lower2.add_next_call(lower2_fn)

        def lower_fn():
            calls.append(31)
            lower.add_next_call(lambda: calls.append(32))

        lower.add_next_call(lower_fn)

        sess.add_next_call(lambda: calls.append(21))

        lower2.execute()

        self.assertEqual([21, 31, 32, 41, 42], calls)
