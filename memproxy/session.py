from __future__ import annotations

from typing import List, Callable, Optional

NextCallFunc = Callable[[], None]


class Session:
    __slots__ = 'next_calls', '_lower', '_higher', 'is_dirty'

    next_calls: List[NextCallFunc]
    _lower: Optional[Session]
    _higher: Optional[Session]
    is_dirty: bool

    def __init__(self):
        self.next_calls = []
        self._lower = None
        self._higher = None
        self.is_dirty = False

    def add_next_call(self, fn: NextCallFunc) -> None:
        self.next_calls.append(fn)

        if self.is_dirty:
            return

        s: Optional[Session] = self
        while s and not s.is_dirty:
            s.is_dirty = True
            s = s._lower

    def execute(self) -> None:
        if not self.is_dirty:
            return

        higher = self._higher
        if higher and higher.is_dirty:
            higher.execute()

        while self.is_dirty:
            call_list = self.next_calls
            self.next_calls = []
            self.is_dirty = False

            for fn in call_list:
                fn()

    def get_lower(self) -> Session:
        if self._lower is None:
            self._lower = Session()
            self._lower._higher = self
        return self._lower
