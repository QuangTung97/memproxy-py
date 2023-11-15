import unittest
from typing import List

from memproxy import Session  # type: ignore


class TestSession(unittest.TestCase):
    def test_simple(self) -> None:
        calls = []
        sess = Session()

        sess.py_add_next(lambda: calls.append('A'))
        sess.py_add_next(lambda: calls.append('B'))
        sess.py_add_next(lambda: calls.append('C'))

        sess.py_execute()

        self.assertEqual(['A', 'B', 'C'], calls)

        calls = []

        sess.py_add_next(lambda: calls.append('E'))
        sess.py_add_next(lambda: calls.append('F'))
        sess.py_execute()

        self.assertEqual(['E', 'F'], calls)

    def test_add_next_call_inside(self) -> None:
        sess = Session()

        calls: List[int] = []

        def handler_02():
            calls.append(12)

        def handler_01():
            sess.py_add_next(handler_02)
            calls.append(11)

        sess.py_add_next(handler_01)

        sess.py_execute()

        self.assertEqual([11, 12], calls)

    def test_lower_session(self) -> None:
        sess = Session()
        lower = sess.py_get_lower()

        self.assertTrue(lower == sess.py_get_lower())
        self.assertTrue(sess != sess.py_get_lower())

        calls: List[int] = []

        lower.py_add_next(lambda: calls.append(31))
        lower.py_add_next(lambda: calls.append(32))

        sess.py_add_next(lambda: calls.append(21))
        sess.py_add_next(lambda: calls.append(22))

        lower.py_execute()

        self.assertEqual([21, 22, 31, 32], calls)

    def test_multi_levels(self) -> None:
        sess = Session()
        lower = sess.py_get_lower()
        lower2 = lower.py_get_lower()

        calls: List[int] = []

        lower2.py_add_next(lambda: calls.append(41))

        lower.py_add_next(lambda: calls.append(31))
        lower.py_add_next(lambda: calls.append(32))

        sess.py_add_next(lambda: calls.append(21))
        sess.py_add_next(lambda: calls.append(22))

        lower2.py_execute()

        self.assertEqual([21, 22, 31, 32, 41], calls)

    def test_execute_at_middle(self) -> None:
        sess = Session()
        lower = sess.py_get_lower()
        lower2 = lower.py_get_lower()

        calls: List[int] = []

        lower2.py_add_next(lambda: calls.append(41))
        lower2.py_add_next(lambda: calls.append(42))

        lower.py_add_next(lambda: calls.append(31))
        lower.py_add_next(lambda: calls.append(32))

        sess.py_add_next(lambda: calls.append(21))
        sess.py_add_next(lambda: calls.append(22))

        lower.py_execute()

        self.assertEqual([21, 22, 31, 32], calls)

        calls = []
        lower.py_execute()
        self.assertEqual([], calls)

        calls = []
        lower2.py_execute()
        self.assertEqual([41, 42], calls)

    def test_multi_levels__add_inside(self) -> None:
        sess = Session()
        lower = sess.py_get_lower()
        lower2 = lower.py_get_lower()

        calls: List[int] = []

        def lower2_fn():
            calls.append(41)
            lower2.py_add_next(lambda: calls.append(42))

        lower2.py_add_next(lower2_fn)

        def lower_fn():
            calls.append(31)
            lower.py_add_next(lambda: calls.append(32))

        lower.py_add_next(lower_fn)

        sess.py_add_next(lambda: calls.append(21))

        lower2.py_execute()

        self.assertEqual([21, 31, 32, 41, 42], calls)
