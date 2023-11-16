cdef class Stats:
    cdef object get_mem_usage(self, int server_id)

    cdef void notify_server_failed(self, int server_id)


cdef class Selector:
    cdef void set_failed_server(self, int server_id)
    cdef tuple select_server(self, str key)
    cdef list select_servers_for_delete(self, str key)
    cdef void reset(self)


cdef class Route:
    cdef Selector new_selector(self)