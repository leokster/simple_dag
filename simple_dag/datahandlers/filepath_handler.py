from simple_dag import storage
from simple_dag.datahandlers import base_handler


class PathInput(base_handler.ABCInput):
    """Hands the resolved path to the transform without reading anything."""

    def __init__(
        self, path, *args, name=None, description=None, health_checks=None, **kwargs
    ) -> None:
        super().__init__(path, name, description, health_checks)
        self.args = args
        self.kwargs = kwargs

    def get_data(self):
        path = storage.resolve(self.path)
        self.check(path, "input")
        return path
