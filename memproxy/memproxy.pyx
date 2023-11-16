from memproxy.session cimport Session


cdef class LeaseGetResponse:
    def __init__(self):
        pass


cdef class LeaseGetResult:
    cdef LeaseGetResponse result(self):
        return None


cdef class LeaseGetResultFunc(LeaseGetResult):
    def __init__(self, object fn):
        self._fn = fn

    cdef LeaseGetResponse result(self):
        return self._fn()


cdef class Pipeline:
    cdef LeaseGetResult lease_get(self, str key):
        return None

    cdef object lease_set(self, str key, size_t cas, bytes data):
        return None

    cdef object delete(self, str key):
        return None

    cdef Session lower_session(self):
        return None

    cdef void finish(self):
        return


cdef class CacheClient:
    cdef Pipeline pipeline(self, Session sess):
        return None