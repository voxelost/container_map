import typing
import pickle
import os

from docker import DockerClient
from docker.models.containers import Container


class ContainerRegistryError(Exception):
    ...


class _CallbackCtxManager:
    def __init__(self, _enter: typing.Callable, _exit: typing.Callable):
        self._enter = _enter
        self._exit = _exit

    def __enter__(self):
        return self._enter()

    def __exit__(self, *_):
        return self._exit()


class _ContainerRegistry:
    _instance = None
    _registry: typing.Dict[typing.Hashable, typing.Any] = {}

    def __new__(cls, *args, **kwargs):
        if not isinstance(cls._instance, cls):
            cls._instance = object.__new__(cls, *args, **kwargs)
        return cls._instance

    def __enter__(self):
        ...

    def __exit__(self, *_):
        registry_keys = list(self._registry.keys())
        for key in registry_keys:
            self.remove(key)

    def block_container_for_ctx(self, c_key: typing.Hashable) -> _CallbackCtxManager:
        container = self.get(c_key)
        if container:
            return _CallbackCtxManager(container.unpause, container.pause)
        else:
            raise ContainerRegistryError(f"couldn't find container {c_key}")

    def get(self, c_key: typing.Hashable) -> Container:
        return self._registry.get(c_key, None)

    def add(self, c_key: typing.Hashable, container: Container) -> None:
        self._registry[c_key] = container

    def remove(self, c_key: typing.Hashable) -> None:
        try:
            print(f"deleting {c_key}")
            self._registry[c_key].unpause()
        finally:
            self._registry[c_key].remove(force=True)
            del self._registry[c_key]


class _ContainerController(DockerClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._registry = _ContainerRegistry()

        self._volume_path = f"{os.path.dirname(os.path.realpath(__file__))}/.vol"
        with open(self._volume_path, "w"):
            ...  # create file if not exists

    def _add_container(self, c_key: typing.Hashable) -> None:

        container = self.containers.run(
            "alpine:3.16",
            detach=True,
            tty=True,
            volumes={
                self._volume_path: {
                    "bind": "/home/data",
                    "mode": "rw",
                }
            },
        )
        container.pause()
        self._registry.add(c_key, container)

    def _set_container_value(self, c_key: typing.Hashable, c_value: typing.Any) -> None:
        if self._registry.get(c_key):
            # dropping the value
            self._registry.remove(c_key)

        self._add_container(c_key)

        with self._registry.block_container_for_ctx(c_key):
            with open(self._volume_path, "wb") as fptr:
                pickle.dump(c_value, fptr)

    def _get_container_value(self, c_key: typing.Hashable) -> typing.Any:
        with self._registry.block_container_for_ctx(c_key):
            with open(self._volume_path, "rb") as fptr:
                c_value = pickle.load(fptr)
                return c_value

    def _remove_container(self, c_key: typing.Hashable) -> None:
        return self._registry.remove(c_key)


class Map(_ContainerRegistry):
    def __init__(self):
        self._controller = _ContainerController.from_env()

    def __enter__(self):
        return self

    def __setitem__(self, key: typing.Hashable, value: typing.Any):
        return self._controller._set_container_value(key, value)

    def __getitem__(self, key: typing.Hashable):
        return self._controller._get_container_value(key)

    def __delitem__(self, key: typing.Hashable):
        return self._controller._remove_container(key)

    def __iter__(self):
        return self._controller._registry._registry.__iter__()


# demo
if __name__ == "__main__":

    def dummy(payload: str, iterations=10):
        i = 0
        while i < iterations:
            yield f"[{i}] hello, {payload}"
            i += 1

    with Map() as map_:
        iterations = 10
        helloer = (lambda: dummy(os.getlogin(), iterations))()

        for _ in range(iterations // 2):
            print(next(helloer))

        map_["hello"] = list(helloer)

        for hello in map_["hello"]:
            print(hello)

        map_["int"] = 0
        for _ in range(10):
            map_["int"] += 1
            print(f'{map_["int"] = }')
