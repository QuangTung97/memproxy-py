cdef class Stats:
    cdef object get_mem_usage(self, int server_id):
        return None

    cdef void notify_server_failed(self, int server_id):
        pass


cdef class Selector:
    cdef void set_failed_server(self, int server_id):
        pass

    cdef tuple select_server(self, str key):
        return None

    cdef list select_servers_for_delete(self, str key):
        return[]

    cdef void reset(self):
        pass


cdef class Route:
    cdef Selector new_selector(self):
        return None
