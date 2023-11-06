from dataclasses import dataclass
from typing import List, Optional

from memproxy import DeleteStatus
from memproxy import Promise, LeaseGetResponse, LeaseSetResponse, DeleteResponse
from memproxy import Session, Pipeline, LeaseSetStatus


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

    def __init__(self):
        self.actions = []

        self.get_keys = []
        self.get_results = []

        self.set_calls = []
        self.finish_calls = 0

    def lease_get(self, key: str) -> Promise[LeaseGetResponse]:
        index = len(self.get_keys)
        self.get_keys.append(key)

        self.actions.append(key)
        global_actions.append(key)

        def get_func():
            self.actions.append(f'{key}:func')
            global_actions.append(f'{key}:func')
            return self.get_results[index]

        return get_func

    def lease_set(self, key: str, cas: int, data: bytes) -> Promise[LeaseSetResponse]:
        self.set_calls.append(SetInput(
            key=key,
            cas=cas,
            val=data,
        ))

        self.actions.append(f'set {key}')

        def set_func() -> LeaseSetResponse:
            self.actions.append(f'set {key}:func')
            return LeaseSetResponse(status=LeaseSetStatus.OK)

        return set_func

    def delete(self, key: str) -> Promise[DeleteResponse]:
        self.actions.append(f'del {key}')
        global_actions.append(f'del {key}')

        def delete_func() -> DeleteResponse:
            self.actions.append(f'del {key}:func')
            global_actions.append(f'del {key}:func')
            return DeleteResponse(status=DeleteStatus.OK)

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
