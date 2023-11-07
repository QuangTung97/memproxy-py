from typing import TypeVar, Generic, List, Any

T = TypeVar("T")


class ObjectPool(Generic[T]):
    _objects: List[T]
    _max_size: int
    _cls: Any

    def __init__(self, clazz: Any, max_size=4096):
        self._objects = []
        self._cls = clazz
        self._max_size = max_size

    def get(self, *args, **kwargs) -> T:
        if len(self._objects) == 0:
            return self._cls(*args, **kwargs)

        obj = self._objects.pop()
        obj.__init__(*args, **kwargs)
        return obj

    def put(self, obj: T):
        if len(self._objects) >= self._max_size:
            return
        self._objects.append(obj)

    def pool_size(self) -> int:
        return len(self._objects)
