from typing import List, Callable

NextCallFunc = Callable[[], None]


class Session:
    _next_calls: List[NextCallFunc]

    def __init__(self):
        self._next_calls = []

    def add_next_call(self, fn: NextCallFunc) -> None:
        self._next_calls.append(fn)

    def execute(self) -> None:
        while True:
            call_list = self._next_calls
            self._next_calls = []

            if len(call_list) == 0:
                return

            for fn in call_list:
                fn()
