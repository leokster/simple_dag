import os

import pandas as pd

from simple_dag.datahandlers import base_handler


def _execute_health_checks(path, df, health_checks):
    for health_check in health_checks:
        if not health_check(df):
            raise ValueError(f"Health check failed for {path}: {health_check.__name__}")


class PandasDFInput(base_handler.ABCInput):
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
            df = pd.read_csv(self.path, **pd_read_csv_kwargs)

            _execute_health_checks(self.path, df, self.health_checks)

            return df

        raise ValueError(f"File extension not supported: {self.path}, only .csv")


class PandasDFOutput(base_handler.ABCOutput):
    def __init__(self, path, name=None, description=None, health_checks=[]) -> None:
        self.path = path
        self.name = name
        self.description = description
        self.health_checks = health_checks

    def write_data(self, df, *args, **kwargs):
        _execute_health_checks(self.path, df, self.health_checks)

        # create parent directories if they don't exist
        os.makedirs(os.path.dirname(self.path), exist_ok=True)

        df.to_csv(self.path, *args, **kwargs)
