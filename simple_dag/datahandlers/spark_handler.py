"""Spark handlers (experimental, might change in the future).

PySpark is optional and not installed with simple_dag: ``pip install pyspark``.
"""

from simple_dag.datahandlers import base_handler

try:
    from pyspark.sql import SparkSession

    SPARK_INSTALLED = True
except ImportError:
    SPARK_INSTALLED = False


class SparkDFInput(base_handler.ABCInput):
    """Reads Parquet, JSON or CSV into a Spark DataFrame."""

    storage_layout = "dataset"
    asset_kind = "table"
    data_format = "parquet"
    default_extension = ".parquet"

    def get_data(self, spark: "SparkSession | None" = None):
        """Read with a Spark session or a context carrying one as ``.spark``."""
        spark = getattr(spark, "spark", spark)
        if spark is None:
            raise ValueError("SparkDFInput requires a Spark session")

        if self.path.endswith(".json"):
            data = spark.read.option("multiLine", "true").json(self.path)
        elif self.path.endswith(".csv"):
            data = spark.read.csv(self.path, header=True, inferSchema=True)
        else:
            # A Parquet file or a directory of part files.
            data = spark.read.parquet(self.path)
        self.check(data, "input")
        return data


class SparkDFOutput(base_handler.ABCOutput):
    """Writes a Spark DataFrame to Parquet."""

    asset_kind = "table"
    data_format = "parquet"
    default_extension = ".parquet"

    def write_data(self, df, mode="overwrite"):
        self.check(df, "output")
        df.write.mode(mode).parquet(self.path)
