from simple_dag.datahandlers import base_handler
from simple_dag.utils import fsspec


class BinaryInput(base_handler.ABCInput):
    """Reads a file as bytes."""

    def __init__(
        self, path, *args, name=None, description=None, health_checks=None, **kwargs
    ) -> None:
        super().__init__(path, name, description, health_checks)
        self.args = args
        self.kwargs = kwargs

    def get_data(self):
        with fsspec.open(self.path, mode="rb") as f:
            data = f.read()
        self.check(data, "input")
        return data


class BinaryOutput(base_handler.ABCOutput):
    """Writes bytes to a file."""

    def write_data(self, data):
        self.check(data, "output")
        with fsspec.open(self.path, mode="wb") as f:
            f.write(data)
