"""Arrow/Parquet handlers with optional pyarrow dependency."""

from simple_dag.checks import evaluate_check
from .base_handler import ABCInput, ABCOutput


class ArrowInput(ABCInput):
    asset_kind = "table"
    data_format = "parquet"
    default_extension = ".parquet"
    storage_layout = "dataset"

    def __init__(
        self,
        path: str,
        *,
        asset_id=None,
        asset_path=None,
        schema_fields=None,
        health_checks=None,
        name=None,
    ):
        self.path, self.asset_id, self.name = path, asset_id, name
        self.asset_path = asset_path or (
            path if path.startswith("/") and asset_id is None else None
        )
        self.schema_fields, self.health_checks = schema_fields or [], health_checks or []

    def get_data(self):
        import pyarrow.parquet as pq

        table = pq.read_table(self.path)
        self.check_results = [evaluate_check(c, table, "input") for c in self.health_checks]
        if any(c.status != "passed" and c.severity == "blocking" for c in self.check_results):
            raise ValueError("Input checks failed")
        return table


class ArrowOutput(ABCOutput):
    asset_kind = "table"
    data_format = "parquet"
    default_extension = ".parquet"

    def __init__(
        self,
        path: str,
        *,
        asset_id=None,
        asset_path=None,
        schema_fields=None,
        health_checks=None,
        name=None,
    ):
        self.path, self.asset_id, self.name = path, asset_id, name
        self.asset_path = asset_path or (
            path if path.startswith("/") and asset_id is None else None
        )
        self.schema_fields, self.health_checks = schema_fields or [], health_checks or []
        self.check_results = []

    def write_data(self, data):
        import pyarrow as pa
        import pyarrow.parquet as pq

        if hasattr(data, "fetch_arrow_table"):
            data = data.fetch_arrow_table()
        if not isinstance(data, pa.Table):
            raise TypeError("ArrowOutput requires an Arrow Table or DuckDB relation")
        self.check_results = [evaluate_check(c, data) for c in self.health_checks]
        if any(c.status != "passed" and c.severity == "blocking" for c in self.check_results):
            raise ValueError("Output checks failed")
        pq.write_table(data, self.path)
