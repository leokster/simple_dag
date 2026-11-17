import pandas as pd

from simple_dag.datahandlers import base_handler
from simple_dag.utils import fsspec


class PandasDFInput(base_handler.ABCInput):
    asset_kind = "table"
    data_format = "csv"
    default_extension = ".csv"

    def __init__(
        self, path, *args, name=None, description=None, health_checks=[], **kwargs
    ) -> None:
        """
        Pandas Dataframe Input Handler.

        Used to read data from pandas dataframes in simple_dag transforms.
        """
        self.path = path
        self.name = name
        self.description = description
        self.args = args
        self.kwargs = kwargs
        self.health_checks = health_checks

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
    asset_kind = "table"
    data_format = "csv"
    default_extension = ".csv"

    def __init__(self, path, name=None, description=None, health_checks=[]) -> None:
        self.path = path
        self.name = name
        self.description = description
        self.health_checks = health_checks

    def write_data(self, df, *args, **kwargs):
        self.check(df, "output")

        with fsspec.open(self.path, "w") as f:
            df.to_csv(f, *args, **kwargs)
