"""Arrow handlers: Parquet in and out as ``pyarrow`` tables.

pyarrow is optional (``pip install simple_dag[arrow]``) and only imported when
data is read or written.
"""

from simple_dag.datahandlers import base_handler


class ArrowInput(base_handler.ABCInput):
    """Reads a Parquet file, or a directory of them, into a ``pyarrow.Table``."""

    asset_kind = "table"
    data_format = "parquet"
    default_extension = ".parquet"
    storage_layout = "dataset"

    def get_data(self):
        import pyarrow.parquet as pq

        table = pq.read_table(self.path)
        self.check(table, "input")
        return table


class ArrowOutput(base_handler.ABCOutput):
    """Writes a ``pyarrow.Table`` or a DuckDB relation to a Parquet file."""

    asset_kind = "table"
    data_format = "parquet"
    default_extension = ".parquet"

    def write_data(self, data):
        import pyarrow as pa
        import pyarrow.parquet as pq

        # A DuckDB relation; newer DuckDB deprecates ``fetch_arrow_table``.
        for to_table in ("to_arrow_table", "fetch_arrow_table"):
            if hasattr(data, to_table):
                data = getattr(data, to_table)()
                break
        if not isinstance(data, pa.Table):
            raise TypeError("ArrowOutput requires an Arrow Table or DuckDB relation")
        self.check(data, "output")
        pq.write_table(data, self.path)
