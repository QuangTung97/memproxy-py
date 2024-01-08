"""
Session implementation.
"""
from __future__ import annotations

from typing import List, Callable, Optional

NextCallFunc = Callable[[], None]


class Session:
    """Session class is for deferring function calls."""

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
        """Add delay call to the list of defer funcs."""
        self.next_calls.append(fn)

        if self.is_dirty:
            return

        s: Optional[Session] = self
        while s and not s.is_dirty:
            s.is_dirty = True
            s = s._lower  # pylint: disable=protected-access

    def execute(self) -> None:
        """
        Execute defer funcs.
        Those defer functions can itself call the add_next_call() inside of them.
        """
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
        """Returns a lower priority session."""
        if self._lower is None:
            self._lower = Session()
            self._lower._higher = self  # pylint: disable=protected-access
        return self._lower
