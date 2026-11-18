import pandas as pd

from simple_dag.datahandlers import base_handler
from simple_dag.utils import fsspec


class PandasDFInput(base_handler.ABCInput):
    """Reads a CSV file into a pandas DataFrame.

    Extra keyword arguments that ``pandas.read_csv`` knows, such as ``sep``, are
    passed on to it.
    """

    asset_kind = "table"
    data_format = "csv"
    default_extension = ".csv"

    def __init__(
        self, path, *args, name=None, description=None, health_checks=None, **kwargs
    ) -> None:
        super().__init__(path, name, description, health_checks)
        self.args = args
        self.kwargs = kwargs

    def get_data(self):
        if self.path.endswith(".csv"):
            pd_read_csv_kwargs = {
                key: val
                for key, val in self.kwargs.items()
                if key in pd.read_csv.__code__.co_varnames
            }
            with fsspec.open(self.path, "r") as f:
                df = pd.read_csv(f, **pd_read_csv_kwargs)

            self.check(df, "input")

            return df

        raise ValueError(f"File extension not supported: {self.path}, only .csv")


class PandasDFOutput(base_handler.ABCOutput):
    """Writes a pandas DataFrame to a CSV file."""

    asset_kind = "table"
    data_format = "csv"
    default_extension = ".csv"

    def write_data(self, df, *args, **kwargs):
        self.check(df, "output")

        with fsspec.open(self.path, "w") as f:
            df.to_csv(f, *args, **kwargs)
