from simple_dag.datahandlers import base_handler

try:
    from pyspark.sql import SparkSession

    SPARK_INSTALLED = True
except ImportError:
    ImportWarning(
        "PySpark not installed. If you want to use Spark please install it with `pip install pyspark`"
    )
    SPARK_INSTALLED = False


class SparkDFInput(base_handler.ABCInput):
    def __init__(self, path, name=None, description=None, health_checks=[]) -> None:
        self.path = path
        self.name = name
        self.description = description

        Warning(
            "Experimental: SparkDFInput is experimental and might change in the future."
        )

    def get_data(self, spark: "SparkSession"):
        if self.path.endswith(".json"):
            return spark.read.option("multiLine", "true").json(self.path)

        if self.path.endswith(".parquet"):
            return spark.read.parquet(self.path)

        if self.path.endswith(".csv"):
            return spark.read.csv(self.path, header=True, inferSchema=True)

        raise ValueError(f"File extension not supported: {self.path}")


class SparkDFOutput(base_handler.ABCOutput):
    def __init__(self, path, name=None, description=None, health_checks=[]) -> None:
        self.path = path
        self.name = name
        self.description = description

    def write_data(self, df, mode="overwrite"):
        df.write.mode(mode).parquet(self.path)
