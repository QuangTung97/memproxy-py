from __future__ import annotations

from typing import List, Callable, Optional

NextCallFunc = Callable[[], None]


class Session:
    __slots__ = '_next_calls', '_lower', '_higher', 'is_dirty'

    _next_calls: List[NextCallFunc]
    _lower: Optional[Session]
    _higher: Optional[Session]
    is_dirty: bool

    def __init__(self):
        self._next_calls = []
        self._lower = None
        self._higher = None
        self.is_dirty = False

    def add_next_call(self, fn: NextCallFunc) -> None:
        self._next_calls.append(fn)

        s: Optional[Session] = self
        while s and not s.is_dirty:
            s.is_dirty = True
            s = s._lower

    def execute(self) -> None:
        higher = self._higher
        if higher and higher.is_dirty:
            higher.execute()

        while True:
            if not self.is_dirty:
                return

            call_list = self._next_calls
            self._next_calls = []
            self.is_dirty = False

            for fn in call_list:
                fn()

    def get_lower(self) -> Session:
        if self._lower is None:
            self._lower = Session()
            self._lower._higher = self
        return self._lower
