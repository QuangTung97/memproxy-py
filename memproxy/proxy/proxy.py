from typing import Dict, List, Callable, Optional

from memproxy import LeaseGetResult
from memproxy import LeaseGetStatus, LeaseSetStatus, DeleteStatus
from memproxy import Pipeline, CacheClient, Session
from memproxy import Promise, LeaseGetResponse, LeaseSetResponse, DeleteResponse
from memproxy.pool import ObjectPool
from .route import Selector, Route


class _ClientConfig:
    clients: Dict[int, CacheClient]
    route: Route

    def __init__(self, clients: Dict[int, CacheClient], route: Route):
        self.clients = clients
        self.route = route


class _LeaseSetServer:
    server_id: Optional[int]

    def __init__(self, server_id: int):
        self.server_id = server_id


class _PipelineConfig:
    conf: _ClientConfig
    sess: Session
    pipe_sess: Session
    selector: Selector

    _pipelines: Dict[int, Pipeline]

    _set_servers: Optional[Dict[str, _LeaseSetServer]]

    def __init__(self, conf: _ClientConfig, sess: Optional[Session]):
        self.conf = conf

        if sess is None:
            sess = Session()

        self.pipe_sess = sess
        self.sess = sess.get_lower()

        self.selector = conf.route.new_selector()

        self._pipelines = {}

        self._set_servers = None

    def get_pipeline(self, server_id: int) -> Pipeline:
        pipe = self._pipelines.get(server_id)
        if pipe:
            return pipe

        new_pipe = self.conf.clients[server_id].pipeline(sess=self.pipe_sess)
        self._pipelines[server_id] = new_pipe
        return new_pipe

    def _get_servers(self) -> Dict[str, _LeaseSetServer]:
        if not self._set_servers:
            self._set_servers = {}
        return self._set_servers

    def add_set_server(self, key: str, server_id: int):
        servers = self._get_servers()
        existing = servers.get(key)
        if existing and existing.server_id != server_id:
            existing.server_id = None
            return

        servers[key] = _LeaseSetServer(server_id=server_id)

    def get_set_server(self, key: str) -> Optional[int]:
        servers = self._get_servers()

        state = servers.get(key)
        if state is None:
            return None

        return state.server_id

    def execute(self):
        self.sess.execute()
        self.selector.reset()

    def finish(self):
        for server_id in self._pipelines:
            self._pipelines[server_id].finish()


class _LeaseGetState:
    conf: _PipelineConfig
    key: str
    server_id: int

    pipe: Pipeline

    fn: LeaseGetResult

    resp: LeaseGetResponse

    def __init__(self, conf: _PipelineConfig, key: str):
        self.conf = conf
        self.key = key

        server_id, _ = conf.selector.select_server(key)
        self.server_id = server_id

        self.pipe = conf.get_pipeline(server_id)

        self.fn = self.pipe.lease_get(key)

    def _handle_resp(self):
        self.resp = self.fn.result()
        if self.resp.status == LeaseGetStatus.LEASE_GRANTED:
            self.conf.add_set_server(self.key, self.server_id)

    def __call__(self) -> None:
        self._handle_resp()

        if self.resp.status != LeaseGetStatus.ERROR:
            return

        self.conf.selector.set_failed_server(self.server_id)

        self.server_id, ok = self.conf.selector.select_server(self.key)
        if not ok:
            return

        pipe = self.conf.get_pipeline(self.server_id)
        self.fn = pipe.lease_get(self.key)

        def next_again_func():
            self._handle_resp()

        self.conf.sess.add_next_call(next_again_func)

    def result(self) -> LeaseGetResponse:
        self.conf.execute()
        resp = self.resp
        lease_get_pool.put(self)
        return resp


lease_get_pool = ObjectPool[_LeaseGetState](clazz=_LeaseGetState)


def new_get_state(conf: _PipelineConfig, key: str) -> _LeaseGetState:
    return lease_get_pool.get(conf, key)


class _LeaseSetState:
    conf: _PipelineConfig
    fn: Promise[LeaseSetResponse]
    resp: LeaseSetResponse

    def __init__(self, conf: _PipelineConfig, fn: Promise[LeaseSetResponse]):
        self.conf = conf
        self.fn = fn

    def next_func(self):
        self.resp = self.fn()

    def return_func(self) -> LeaseSetResponse:
        self.conf.execute()
        return self.resp


class _DeleteState:
    conf: _PipelineConfig

    fn_list: List[Promise[DeleteResponse]]
    servers: List[int]

    resp: DeleteResponse

    def __init__(self, conf: _PipelineConfig, fn_list: List[Promise[DeleteResponse]], servers: List[int]):
        self.conf = conf
        self.fn_list = fn_list
        self.servers = servers

    def next_func(self) -> None:
        resp = DeleteResponse(status=DeleteStatus.NOT_FOUND)
        for i, resp_fn in enumerate(self.fn_list):
            new_resp = resp_fn()
            if new_resp.status == DeleteStatus.OK:
                resp = new_resp
            elif new_resp.status == DeleteStatus.ERROR:
                server_id = self.servers[i]
                self.conf.selector.set_failed_server(server_id=server_id)

        self.resp = resp

    def return_func(self) -> DeleteResponse:
        self.conf.execute()
        return self.resp


class ProxyPipeline:
    _conf: _PipelineConfig

    def __init__(self, conf: _ClientConfig, sess: Optional[Session]):
        self._conf = _PipelineConfig(conf=conf, sess=sess)

    def lease_get(self, key: str) -> LeaseGetResult:
        state = new_get_state(
            conf=self._conf,
            key=key,
        )

        self._conf.sess.add_next_call(state)
        return state

    def lease_set(self, key: str, cas: int, data: bytes) -> Promise[LeaseSetResponse]:
        server_id = self._conf.get_set_server(key)
        if not server_id:
            def lease_set_error() -> LeaseSetResponse:
                return LeaseSetResponse(status=LeaseSetStatus.ERROR, error='proxy: can not do lease set')

            return lease_set_error

        pipe = self._conf.get_pipeline(server_id)

        fn = pipe.lease_set(key, cas, data)
        state = _LeaseSetState(conf=self._conf, fn=fn)

        self._conf.sess.add_next_call(state.next_func)

        return state.return_func

    def delete(self, key: str) -> Promise[DeleteResponse]:
        servers = self._conf.selector.select_servers_for_delete(key)

        fn_list: List[Promise[DeleteResponse]] = []
        for server_id in servers:
            pipe = self._conf.get_pipeline(server_id)
            fn = pipe.delete(key)
            fn_list.append(fn)

        state = _DeleteState(conf=self._conf, fn_list=fn_list, servers=servers)
        self._conf.sess.add_next_call(state.next_func)

        return state.return_func

    def lower_session(self) -> Session:
        return self._conf.sess.get_lower()

    def finish(self) -> None:
        self._conf.finish()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finish()


class ProxyCacheClient:
    _conf: _ClientConfig

    def __init__(
            self,
            server_ids: List[int],
            new_func: Callable[[int], CacheClient],  # server_id -> CacheClient
            route: Route,
    ):
        clients: Dict[int, CacheClient] = {}
        for server_id in server_ids:
            client = new_func(server_id)
            clients[server_id] = client

        self._conf = _ClientConfig(
            clients=clients,
            route=route,
        )

    def pipeline(self, sess: Optional[Session] = None) -> Pipeline:
        return ProxyPipeline(conf=self._conf, sess=sess)
