from dataclasses import dataclass
from typing import List, Optional

from memproxy import DeleteStatus
from memproxy import LeaseGetResult
from memproxy import Promise, LeaseGetResponse, LeaseSetResponse, DeleteResponse
from memproxy import Session, Pipeline, LeaseSetStatus
from memproxy.memproxy import LeaseGetResultFunc


@dataclass
class SetInput:
    key: str
    cas: int
    val: bytes


global_actions: List[str] = []


class PipelineFake:
    actions: List[str]

    get_keys: List[str]
    get_results: List[LeaseGetResponse]

    set_calls: List[SetInput]

    sess: Session

    finish_calls: int

    delete_resp: DeleteResponse

    def append_action(self, action: str):
        self.actions.append(action)
        global_actions.append(action)

    def __init__(self):
        self.actions = []
        self.sess = Session()

        self.get_keys = []
        self.get_results = []

        self.set_calls = []
        self.finish_calls = 0

        self.delete_resp = DeleteResponse(status=DeleteStatus.OK)

    def lease_get(self, key: str) -> LeaseGetResult:
        index = len(self.get_keys)
        self.get_keys.append(key)

        self.append_action(key)

        def get_func():
            self.append_action(f'{key}:func')
            return self.get_results[index]

        return LeaseGetResultFunc(get_func)

    def lease_set(self, key: str, cas: int, data: bytes) -> Promise[LeaseSetResponse]:
        self.set_calls.append(SetInput(
            key=key,
            cas=cas,
            val=data,
        ))

        self.append_action(f'set {key}')

        def set_func() -> LeaseSetResponse:
            self.append_action(f'set {key}:func')
            return LeaseSetResponse(status=LeaseSetStatus.OK)

        return set_func

    def delete(self, key: str) -> Promise[DeleteResponse]:
        self.append_action(f'del {key}')

        def delete_func() -> DeleteResponse:
            self.append_action(f'del {key}:func')
            return self.delete_resp

        return delete_func

    def lower_session(self) -> Session:
        return self.sess

    def finish(self) -> None:
        self.actions.append('finish')
        global_actions.append('finish')
        self.finish_calls += 1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finish()


class ClientFake:
    new_calls: List[Optional[Session]]
    pipe: PipelineFake

    def __init__(self):
        self.new_calls = []
        self.pipe = PipelineFake()

    def pipeline(self, sess: Optional[Session] = None) -> Pipeline:
        self.new_calls.append(sess)
        return self.pipe
