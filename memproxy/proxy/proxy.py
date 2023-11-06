from typing import Dict, List, Callable, Optional

from memproxy import Pipeline, CacheClient, Session
from memproxy import Promise, LeaseGetResponse, LeaseSetResponse, DeleteResponse
from .route import Selector, Route


class _ClientConfig:
    clients: Dict[int, CacheClient]
    route: Route

    def __init__(self, clients: Dict[int, CacheClient], route: Route):
        self.clients = clients
        self.route = route


class _PipelineConfig:
    conf: _ClientConfig
    sess: Session
    pipe_sess: Session
    selector: Selector
    pipelines: Dict[int, Pipeline]

    def __init__(self, conf: _ClientConfig, sess: Optional[Session]):
        self.conf = conf

        if sess is None:
            sess = Session()

        self.pipe_sess = sess
        self.sess = sess.get_lower()

        self.selector = conf.route.new_selector()
        self.pipelines = {}

    def get_pipeline(self, server_id: int) -> Pipeline:
        pipe = self.pipelines.get(server_id)
        if pipe:
            return pipe

        new_pipe = self.conf.clients[server_id].pipeline(sess=self.pipe_sess)
        self.pipelines[server_id] = new_pipe
        return new_pipe


class _LeaseSetState:
    server_id: Optional[int]


class _LeaseGetState:
    conf: _PipelineConfig
    key: str
    server_id: int

    pipe: Pipeline

    fn: Promise[LeaseGetResponse]

    resp: LeaseGetResponse

    def __init__(self, conf: _PipelineConfig, key: str):
        self.conf = conf
        self.key = key

        server_id, _ = conf.selector.select_server(key)
        self.server_id = server_id

        self.pipe = conf.get_pipeline(server_id)

        self.fn = self.pipe.lease_get(key)

    def next_func(self) -> None:
        resp = self.fn()
        self.resp = resp

    def return_func(self) -> LeaseGetResponse:
        self.conf.sess.execute()
        self.conf.selector.reset()

        return self.resp


class ProxyPipeline:
    _conf: _PipelineConfig

    def __init__(self, conf: _ClientConfig, sess: Optional[Session]):
        self._conf = _PipelineConfig(conf=conf, sess=sess)

    def lease_get(self, key: str) -> Promise[LeaseGetResponse]:
        state = _LeaseGetState(
            conf=self._conf,
            key=key,
        )

        return state.return_func

    def lease_set(self, key: str, cas: int, data: bytes) -> Promise[LeaseSetResponse]:
        raise NotImplemented

    def delete(self, key: str) -> Promise[DeleteResponse]:
        raise NotImplemented

    def lower_session(self) -> Session:
        return self._conf.sess.get_lower()

    def finish(self) -> None:
        pass

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
