cdef class Session:
    def __init__(self):
        self._next_calls = []
        self._lower = None
        self._higher = None
        self.is_dirty = False
    
    cdef void add_next_call(self, object fn):
        self._next_calls.append(fn)

        cdef Session s = self
        while s and not s.is_dirty:
            s.is_dirty = True
            s = s._lower

    def py_add_next(self, object fn):
        self.add_next_call(fn)

    cdef void execute(self):
        cdef Session higher = self._higher
        cdef list call_list

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

    def py_execute(self):
        self.execute()

    cdef Session get_lower(self):
        if self._lower is None:
            self._lower = Session()
            self._lower._higher = self
        return self._lower
    
    def py_get_lower(self):
        return self.get_lower()
