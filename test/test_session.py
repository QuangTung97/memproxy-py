import unittest

from memproxy import Session


class TestSample(unittest.TestCase):
    def test_hello(self):
        self.assertEqual('3', '3')

    def test_tung(self):
        self.assertEqual(5, 5)


class TestSession(unittest.TestCase):
    def test_simple(self):
        calls = []
        sess = Session()

        sess.add_next_call(lambda: calls.append('A'))
        sess.add_next_call(lambda: calls.append('B'))
        sess.add_next_call(lambda: calls.append('C'))

        sess.execute()

        self.assertEqual(['A', 'B', 'C'], calls)
