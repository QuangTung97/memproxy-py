from memproxy.session cimport Session


cdef enum LeaseGetStatus:
    LEASE_GET_OK = 1
    LEASE_GET_LEASE_GRANTED = 2
    LEASE_GET_ERROR = 3


cdef class LeaseGetResponse:
    cdef:
        public LeaseGetStatus status
        public bytes data
        public size_t cas
        public str error


cdef enum LeaseSetStatus:
    LEASE_SET_OK = 1
    LEASE_SET_ERROR = 2
    LEASE_SET_NOT_FOUND = 3  # key not found
    LEASE_SET_CAS_MISMATCH = 4


cdef class LeaseSetResponse:
    cdef:    
        public LeaseSetStatus status
        public str error


cdef enum DeleteStatus:
    DELETE_OK = 1
    DELETE_ERROR = 2
    DELETE_NOT_FOUND = 3  # key not found


cdef class DeleteResponse:
    cdef:
        public DeleteStatus status
        public str error


cdef class LeaseGetResult:
    cdef LeaseGetResponse result(self)


cdef class LeaseGetResultFunc(LeaseGetResult):
    cdef:
        object _fn

    cdef LeaseGetResponse result(self)


cdef class Pipeline:
    cdef LeaseGetResult lease_get(self, str key)

    cdef object lease_set(self, str key, size_t cas, bytes data)

    cdef object delete(self, str key)

    cdef Session lower_session(self)

    cdef void finish(self)

cdef class CacheClient:
    cdef Pipeline pipeline(self, Session)