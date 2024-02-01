import json
from typing import Type
import fsspec

from pydantic import BaseModel

from simple_dag.datahandlers import base_handler


def _execute_health_checks(path, df, health_checks):
    for health_check in health_checks:
        if not health_check(df):
            raise ValueError(f"Health check failed for {path}: {health_check.__name__}")


class JsonInput(base_handler.ABCInput):
    def __init__(
        self,
        path,
        *args,
        name=None,
        description=None,
        health_checks=[],
        schema_validation: Type[BaseModel] = None,
        return_pydantic_model=True,
        **kwargs,
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
        self.schema_validation = schema_validation
        self.return_pydantic_model = return_pydantic_model

    def get_data(self):
        if self.path.endswith(".json"):
            data = {}
            with fsspec.open(self.path, "r") as f:
                data = json.load(f)

            pydantic_data = None
            # if schema_validation is not None check pydantic schema
            if self.schema_validation is not None:
                pydantic_data = self.schema_validation.model_validate(data)

            if pydantic_data is not None and self.return_pydantic_model:
                _execute_health_checks(self.path, pydantic_data, self.health_checks)
                return pydantic_data

            elif not self.return_pydantic_model and pydantic_data is None:
                raise ValueError(
                    f"You need to specify a pydantic model to validate the data in {self.path}"
                )

            _execute_health_checks(self.path, data, self.health_checks)
            return data

        raise ValueError(f"File extension not supported: {self.path}, only .json")


class JsonOutput(base_handler.ABCOutput):
    def __init__(
        self,
        path,
        name=None,
        description=None,
        health_checks=[],
        schema_validation=None,
    ) -> None:
        self.path = path
        self.name = name
        self.description = description
        self.health_checks = health_checks
        self.schema_validation = schema_validation

    def write_data(self, data, *args, **kwargs):
        if isinstance(data, BaseModel):
            json_data = data.model_dump()

        elif isinstance(data, dict) or isinstance(data, list):
            json_data = data

        else:
            raise ValueError(
                f"Data type not supported: {type(data)}, only pydantic models and dicts/lists"
            )

        _execute_health_checks(self.path, data, self.health_checks)

        with fsspec.open(self.path, "w") as f:
            json.dump(json_data, f, indent=2, *args, **kwargs)
