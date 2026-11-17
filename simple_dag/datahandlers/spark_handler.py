from simple_dag.datahandlers import base_handler

try:
    from pyspark.sql import SparkSession

    SPARK_INSTALLED = True
except ImportError:
    ImportWarning(
        "PySpark not installed. If you want to use Spark please install it "
        "with `pip install pyspark`"
    )
    SPARK_INSTALLED = False


class SparkDFInput(base_handler.ABCInput):
    storage_layout = "dataset"
    asset_kind = "table"
    data_format = "parquet"
    default_extension = ".parquet"

    def __init__(self, path, name=None, description=None, health_checks=[]) -> None:
        self.path = path
        self.name = name
        self.description = description
        self.health_checks = health_checks

        Warning("Experimental: SparkDFInput is experimental and might change in the future.")

    def get_data(self, spark: "SparkSession"):
        if hasattr(spark, "spark"):
            spark = spark.spark
        if spark is None:
            raise ValueError("SparkDFInput requires a Spark session")
        if self.path.endswith(".json"):
            data = spark.read.option("multiLine", "true").json(self.path)
            self.check(data, "input")
            return data

        if self.path.endswith(".parquet") or not self.path.endswith((".json", ".csv")):
            data = spark.read.parquet(self.path)
            self.check(data, "input")
            return data

        if self.path.endswith(".csv"):
            data = spark.read.csv(self.path, header=True, inferSchema=True)
            self.check(data, "input")
            return data

        raise ValueError(f"File extension not supported: {self.path}")


class SparkDFOutput(base_handler.ABCOutput):
    asset_kind = "table"
    data_format = "parquet"
    default_extension = ".parquet"

    def __init__(self, path, name=None, description=None, health_checks=[]) -> None:
        self.path = path
        self.name = name
        self.description = description
        self.health_checks = health_checks

    def write_data(self, df, mode="overwrite"):
        self.check(df, "output")
        df.write.mode(mode).parquet(self.path)
