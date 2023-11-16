from typing import Dict, List, Callable, Optional

from memproxy.memproxy cimport LeaseGetResult
from memproxy.memproxy cimport LeaseSetStatus, DeleteStatus
from memproxy.memproxy cimport Pipeline, CacheClient 
from memproxy.memproxy cimport LeaseGetResponse, LeaseSetResponse, DeleteResponse

from memproxy.proxy.route cimport Selector, Route
from memproxy.session cimport Session


cdef class _ClientConfig:
    cdef:
        dict clients
        Route route

    def __init__(self, dict clients, Route route):
        self.clients = clients
        self.route = route


cdef class _LeaseSetServer:
    cdef:
        object server_id

    def __init__(self, int server_id):
        self.server_id = server_id


cdef class _PipelineConfig:
    cdef:
        _ClientConfig conf
        Session sess
        Session pipe_sess
        Selector selector

        dict _pipelines

        dict _set_servers

        Pipeline pipe
        object server_id

    def __init__(self, _ClientConfig conf, Session sess):
        self.conf = conf

        if sess is None:
            sess = Session()

        self.pipe_sess = sess
        self.sess = sess.get_lower()

        self.selector = conf.route.new_selector()

        self._pipelines = {}

        self._set_servers = None

        self.pipe = None
        self.server_id = None

    cdef Pipeline get_pipeline(self, int server_id):
        cdef Pipeline pipe = self._pipelines.get(server_id)
        self.pipe = pipe
        if pipe:
            return pipe

        cdef Pipeline new_pipe = self.conf.clients[server_id].pipeline(sess=self.pipe_sess)
        self._pipelines[server_id] = new_pipe
        self.pipe = new_pipe
        return new_pipe

    cdef dict _get_servers(self):
        if not self._set_servers:
            self._set_servers = {}
        return self._set_servers

    cdef void add_set_server(self, str key, int server_id):
        cdef dict servers = self._get_servers()

        cdef _LeaseSetServer existing = servers.get(key)

        if existing and existing.server_id != server_id:
            existing.server_id = None
            return

        servers[key] = _LeaseSetServer(server_id=server_id)

    cdef object get_set_server(self, str key):
        cdef dict servers = self._get_servers()

        cdef _LeaseSetServer state = servers.get(key)
        if state is None:
            return None

        return state.server_id

    cdef void dexecute(self):
        self.pipe = None
        self.server_id = None
        self.sess.execute()
        self.selector.reset()

    cdef void finish(self):
        cdef object server_id
        for server_id in self._pipelines:
            self._pipelines[server_id].finish()


cdef class _LeaseGetState(LeaseGetResult):
    cdef:
        _PipelineConfig conf
        str key
        int server_id

        Pipeline pipe

        LeaseGetResult fn

        LeaseGetResponse resp

    cdef _handle_resp(self):
        self.resp = self.fn.result()
        if self.resp[0] == 2:
            self.conf.add_set_server(self.key, self.server_id)

    def __call__(self):
        self.resp = self.fn.result()

        if self.resp[0] == 1:
            return

        if self.resp[0] == 2:
            self.conf.add_set_server(self.key, self.server_id)
            return

        self.conf.selector.set_failed_server(self.server_id)

        self.server_id, ok = self.conf.selector.select_server(self.key)
        if not ok:
            return
        self.conf.server_id = self.server_id

        pipe = self.conf.get_pipeline(self.server_id)
        self.fn = pipe.lease_get(self.key)

        def next_again_func():
            self._handle_resp()

        self.conf.sess.add_next_call(next_again_func)

    cdef LeaseGetResponse result(self):
        if self.conf.sess.is_dirty:
            self.conf.execute()

        cdef LeaseGetResponse resp = self.resp

        if len(_P) < 4096:
            _P.append(self)

        return resp


cdef list get_state_pool = []
cdef list _P = get_state_pool


cdef class _LeaseSetState:
    cdef:
        _PipelineConfig conf
        object fn
        LeaseSetResponse resp

    def __init__(self, _PipelineConfig conf, object fn):
        self.conf = conf
        self.fn = fn

    def next_func(self):
        self.resp = self.fn()

    def return_func(self):
        self.conf.execute()
        return self.resp


cdef class _DeleteState:
    cdef:
        _PipelineConfig conf
        list fn_list
        list servers
        DeleteResponse resp

    def __init__(self, _PipelineConfig conf, list fn_list, list servers):
        self.conf = conf
        self.fn_list = fn_list
        self.servers = servers

    def next_func(self):
        cdef DeleteResponse resp = DeleteResponse(status=DeleteStatus.DELETE_NOT_FOUND)
        cdef size_t i
        cdef DeleteResponse new_resp

        for i, resp_fn in enumerate(self.fn_list):
            new_resp = resp_fn()
            if new_resp.status == DeleteStatus.DELETE_OK:
                resp = new_resp
            elif new_resp.status == DeleteStatus.DELETE_ERROR:
                server_id = self.servers[i]
                self.conf.selector.set_failed_server(server_id=server_id)

        self.resp = resp

    def return_func(self):
        self.conf.execute()
        return self.resp


cdef class ProxyPipeline(Pipeline):
    cdef:
        _PipelineConfig _conf

    def __init__(self, _ClientConfig conf, Session sess):
        self._conf = _PipelineConfig(conf=conf, sess=sess)

    cdef LeaseGetResult lease_get(self, str key):
        cdef _LeaseGetState state
        if len(_P) == 0:
            state = _LeaseGetState()
        else:
            state = _P.pop()

        # do init get state
        cdef _PipelineConfig conf = self._conf
        state.conf = conf
        state.key = key

        if conf.pipe:
            state.pipe = conf.pipe
            state.server_id = conf.server_id  # type: ignore
        else:
            server_id, _ = conf.selector.select_server(key)
            state.server_id = server_id
            conf.server_id = server_id

            state.pipe = conf.get_pipeline(server_id)

        state.fn = state.pipe.lease_get(key)
        # end init get state

        self._conf.sess.add_next_call(state)
        return state

    cdef object lease_set(self, str key, size_t cas, bytes data):
        cdef object server_id = self._conf.get_set_server(key)
        if not server_id:
            def lease_set_error() -> LeaseSetResponse:
                return LeaseSetResponse(status=LeaseSetStatus.LEASE_SET_ERROR, error='proxy: can not do lease set')

            return lease_set_error

        cdef Pipeline pipe = self._conf.get_pipeline(server_id)

        fn = pipe.lease_set(key, cas, data)
        state = _LeaseSetState(conf=self._conf, fn=fn)

        self._conf.sess.add_next_call(state.next_func)

        return state.return_func

    cdef object delete(self, str key):
        servers = self._conf.selector.select_servers_for_delete(key)

        cdef list fn_list = []
        for server_id in servers:
            pipe = self._conf.get_pipeline(server_id)
            fn = pipe.delete(key)
            fn_list.append(fn)

        cdef _DeleteState state = _DeleteState(conf=self._conf, fn_list=fn_list, servers=servers)
        self._conf.sess.add_next_call(state.next_func)

        return state.return_func

    cdef Session lower_session(self):
        return self._conf.sess.get_lower()

    cdef void finish(self):
        self._conf.finish()


cdef class ProxyCacheClient(CacheClient):
    cdef:
        _ClientConfig _conf

    def __init__(
            self,
            list server_ids, # list of int
            object new_func,  # server_id -> CacheClient
            Route route,
    ):
        cdef dict clients = {}
        cdef int server_id
        for server_id in server_ids:
            client = new_func(server_id)
            clients[server_id] = client

        self._conf = _ClientConfig(
            clients=clients,
            route=route,
        )

    cdef Pipeline pipeline(self, Session sess):
        return ProxyPipeline(conf=self._conf, sess=sess)
