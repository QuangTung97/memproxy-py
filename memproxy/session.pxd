cdef class Session:
    cdef:
        list _next_calls
        Session _lower
        Session _higher
        bint is_dirty

    cdef void add_next_call(self, object fn)

    cdef void execute(self)

    cdef Session get_lower(self)