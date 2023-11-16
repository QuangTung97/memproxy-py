from enum import Enum

from memproxy.item import Item, new_json_codec, ItemCodec, new_multi_get_filler  # type: ignore
from memproxy.memproxy import CacheClient, Pipeline  # type: ignore
from memproxy.memproxy import LeaseGetResponse, LeaseSetResponse, DeleteResponse  # type: ignore
from memproxy.memproxy import LeaseGetResult  # type: ignore
from memproxy.redis import RedisClient  # type: ignore
from memproxy.session import Session  # type: ignore


class LeaseSetStatus(Enum):
    OK = 1
    ERROR = 2
    NOT_FOUND = 3  # key not found
    CAS_MISMATCH = 4


class DeleteStatus(Enum):
    OK = 1
    ERROR = 2
    NOT_FOUND = 3  # key not found
