from typing import List, Callable

NextCallFunc = Callable[[], None]


class Session:
    _next_calls: List[NextCallFunc]

    def __init__(self):
        self._next_calls = []

    def add_next_call(self, fn: NextCallFunc):
        self._next_calls.append(fn)

    def execute(self):
        call_list = self._next_calls
        self._next_calls = []

        for fn in call_list:
            fn()
