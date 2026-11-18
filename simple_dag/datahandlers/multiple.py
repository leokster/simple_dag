from simple_dag.datahandlers import base_handler
from simple_dag.utils import fsspec


class Multiple(base_handler.ABCInput):
    """Reads every file matching a glob pattern with the wrapped input.

    The transform receives a list with one entry per file. The wrapped input is
    rebound to each match, so its options and health checks apply to every file.
    """

    storage_layout = "pattern"

    def __init__(self, input_obj: base_handler.ABCInput) -> None:
        super().__init__(
            input_obj.path,
            input_obj.name,
            input_obj.description,
            asset_path=input_obj.asset_path,
        )
        self.asset_kind = input_obj.asset_kind
        self.data_format = input_obj.data_format
        self.template = input_obj.bind_path(input_obj.path)

        # For subclasses that still construct one input per file themselves.
        self.input_cls = input_obj.__class__
        self.args = getattr(input_obj, "args", ())
        self.kwargs = dict(getattr(input_obj, "kwargs", {}))
        if input_obj.health_checks is not base_handler.DataHandler.health_checks:
            self.kwargs["health_checks"] = input_obj.health_checks

    def _inputs(self) -> list[base_handler.ABCInput]:
        # self.path contains wildcards e.g. /path/to/data/*.csv
        return [self.template.bind_path(file) for file in fsspec.list_files(self.path)]

    def get_data(self):
        return [input_obj.get_data() for input_obj in self._inputs()]


class LengthAwareIterator:
    def __init__(self, inputs: list[base_handler.ABCInput]):
        self.inputs = inputs
        self.length = len(inputs)

    def __iter__(self):
        for input_obj in self.inputs:
            yield input_obj.get_data()

    def __len__(self):
        return self.length


class MultipleIterator(Multiple):
    """Like ``Multiple``, but reads each file only when it is iterated over."""

    def get_data(self):
        return LengthAwareIterator(self._inputs())
