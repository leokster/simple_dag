from simple_dag.datahandlers import base_handler
from simple_dag.utils import fsspec


class Multiple(base_handler.ABCInput):
    def __init__(self, input_obj: base_handler.ABCInput) -> None:
        self.path = input_obj.path
        self.name = input_obj.name
        self.description = input_obj.description
        if hasattr(input_obj, "args"):
            self.args = input_obj.args
        else:
            self.args = ()
        if hasattr(input_obj, "kwargs"):
            self.kwargs = input_obj.kwargs
        else:
            self.kwargs = {}
        if hasattr(input_obj, "health_checks"):
            self.kwargs["health_checks"] = input_obj.health_checks

        self.input_cls = input_obj.__class__

    def get_data(self):
        # self.path contains wildcards e.g. /path/to/data/*.csv
        # we need to use fsspec to get the list of files
        files = fsspec.list_files(self.path)
        input_objs = []
        for file in files:
            input_obj = self.input_cls(file, *self.args, **self.kwargs)
            input_objs.append(input_obj)

        return [input_obj.get_data() for input_obj in input_objs]
