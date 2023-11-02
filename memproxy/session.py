from __future__ import annotations

from typing import List, Callable, Optional

NextCallFunc = Callable[[], None]


class Session:
    _next_calls: List[NextCallFunc]
    _lower: Optional[Session]
    _higher: Optional[Session]
    _is_dirty: bool

    def __init__(self):
        self._next_calls = []
        self._lower = None
        self._higher = None
        self._is_dirty = False

    def add_next_call(self, fn: NextCallFunc) -> None:
        self._next_calls.append(fn)
        self._set_dirty_recursive()

    def execute(self) -> None:
        higher = self._higher
        if higher and higher._is_dirty:
            higher.execute()

        while True:
            call_list = self._next_calls
            self._next_calls = []
            self._is_dirty = False

            if len(call_list) == 0:
                return

            for fn in call_list:
                fn()

    def _set_dirty_recursive(self):
        s = self
        while s and not s._is_dirty:
            s._is_dirty = True
            s = s._lower

    def get_lower(self) -> Session:
        if self._lower is None:
            self._lower = Session()
            self._lower._higher = self
        return self._lower
