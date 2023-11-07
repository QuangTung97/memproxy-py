from typing import TypeVar, Generic, List, Callable

T = TypeVar("T")


class ObjectPool(Generic[T]):
    _objects: List[T]
    _max_size: int
    _new_func: Callable[[], T]

    def __init__(self, new_func: Callable[[], T], max_size=4096):
        self._objects = []
        self._max_size = max_size
        self._new_func = new_func

    def get(self) -> T:
        if len(self._objects) == 0:
            return self._new_func()
        return self._objects.pop()

    def put(self, obj: T):
        if len(self._objects) >= self._max_size:
            return
        self._objects.append(obj)

    def pool_size(self) -> int:
        return len(self._objects)
