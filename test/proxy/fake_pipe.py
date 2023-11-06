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


class PipelineFake:
    get_calls: List[str]
    get_results: List[LeaseGetResponse]

    set_calls: List[SetInput]

    delete_calls: List[str]

    sess: Session

    def __init__(self):
        self.get_calls = []
        self.get_results = []

        self.set_calls = []

        self.delete_calls = []

    def lease_get(self, key: str) -> Promise[LeaseGetResponse]:
        index = len(self.get_calls)
        self.get_calls.append(key)

        def get_func():
            return self.get_results[index]

        return get_func

    def lease_set(self, key: str, cas: int, data: bytes) -> Promise[LeaseSetResponse]:
        self.set_calls.append(SetInput(
            key=key,
            cas=cas,
            val=data,
        ))

        def set_func() -> LeaseSetResponse:
            return LeaseSetResponse(status=LeaseSetStatus.OK)

        return set_func

    def delete(self, key: str) -> Promise[DeleteResponse]:
        self.delete_calls.append(key)

        def delete_func() -> DeleteResponse:
            return DeleteResponse(status=DeleteStatus.OK)

        return delete_func

    def lower_session(self) -> Session:
        return self.sess

    def finish(self) -> None:
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finish()


class ClientFake:
    new_calls: List[Optional[Session]]
    pipe: PipelineFake

    def __init__(self):
        self.new_calls = []

    def pipeline(self, sess: Optional[Session] = None) -> Pipeline:
        self.new_calls.append(sess)
        self.pipe = PipelineFake()
        return self.pipe
