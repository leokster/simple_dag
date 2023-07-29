import os
import sys
from importlib import import_module
from pathlib import Path
from typing import Callable, List

from simple_dag.datahandlers.binary_handler import BinaryInput, BinaryOutput
from simple_dag.datahandlers.pandas_handler import PandasDFInput, PandasDFOutput
from simple_dag.datahandlers.spark_handler import SparkDFInput, SparkDFOutput

try:
    from pyspark.sql import SparkSession

    SPARK_INSTALLED = True
except ImportError:
    ImportWarning(
        "PySpark not installed. If you want to use Spark please install it with `pip install pyspark`"
    )
    SPARK_INSTALLED = False


def get_spark():
    spark = (
        SparkSession.builder.appName("news_gatherer")
        .master("local[*]")  # Use all available cores
        .config("spark.executor.memory", "4g")  # Set executor memory to 4GB
        .config("spark.driver.memory", "2g")  # Set driver memory to 2GB
        .config("spark.driver.maxResultSize", "1g")  # Set maximum result size to 1GB
        .getOrCreate()
    )

    # Set the AWS credentials and endpoint URL
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"]
    )
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"]
    )
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.s3a.endpoint", os.environ["AWS_ENDPOINT_URL"]
    )
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )

    return spark


class Transform:
    compute_func: Callable = None
    on_upstream_success: bool = False

    def __init__(self, compute_func, *args, **kwargs) -> None:
        self.compute_func = compute_func
        self.args = args
        self.kwargs = kwargs

    @property
    def cron(self):
        if not hasattr(self, "_cron"):
            return []
        return self._cron

    def set_cron(self, cron: str):
        if not hasattr(self, "_cron"):
            self._cron = []
        self._cron.append(cron)

    def compute(self, ctx):
        parsed_args = self.args
        parsed_kwargs = {}

        for key, val in self.kwargs.items():
            if isinstance(val, SparkDFInput):
                if ctx is None:
                    raise ValueError(
                        "Spark context is None. Please provide a Spark context to the compute method"
                    )
                parsed_kwargs[key] = val.get_data(ctx)

            elif isinstance(val, SparkDFOutput):
                parsed_kwargs[key] = val

            elif isinstance(val, PandasDFInput):
                parsed_kwargs[key] = val.get_data()

            elif isinstance(val, PandasDFOutput):
                parsed_kwargs[key] = val

            elif isinstance(val, BinaryInput):
                parsed_kwargs[key] = val.get_data()

            elif isinstance(val, BinaryOutput):
                parsed_kwargs[key] = val

        if "ctx" in self.compute_func.__code__.co_varnames:
            parsed_kwargs["ctx"] = ctx

        return self.compute_func(*parsed_args, **parsed_kwargs)

    def __call__(self):
        if SPARK_INSTALLED:
            ctx = get_spark()
        else:
            ctx = None

        result = self.compute(ctx)

        if ctx is not None:
            ctx.stop()

        return result


def transform(*args, **kwargs):
    def _inner(func):
        trans = Transform(func, *args, **kwargs)
        trans.__name__ = func.__name__
        return trans

    return _inner


def schedule(cron: str = None, on_upstream_success: bool = False):
    def _inner(obj: Transform):
        if not isinstance(obj, Transform):
            raise ValueError("schedule decorator can only be used on Transform objects")
        if cron:
            obj.set_cron(cron)
        if on_upstream_success:
            obj.on_upstream_success = on_upstream_success
        return obj

    return _inner


def find_transform_instances_in_module(module):
    transform_instances = []

    for name, obj in module.__dict__.items():
        if isinstance(obj, Transform):
            transform_instances.append(obj)

    return transform_instances


def find_transform_instances_in_folder(transforms_folder):
    """
    Find all Transform instances in a folder

    Args:
        transforms_folder (str): The path to the folder containing the transforms

    Example:
        >>> TRANSFORMS_FOLDER = os.path.join(os.path.dirname(__file__), "../transforms")
        >>> all_transforms = find_transform_instances_in_folder(TRANSFORMS_FOLDER)
        >>> for transforms in all_transforms:
        >>>     print(transforms.__name__)

    """
    root_path = Path(transforms_folder)
    all_transforms = []

    for file_path in root_path.glob("**/*.py"):
        # Derive the module name from the file path
        module_name = (
            file_path.relative_to(transforms_folder)
            .with_suffix("")
            .as_posix()
            .replace("/", ".")
        )

        # Add the root folder to the sys.path if it's not already in it
        if transforms_folder not in sys.path:
            sys.path.insert(0, transforms_folder)

        # Import the module and find Transform instances
        module = import_module(module_name)
        transform_instances = find_transform_instances_in_module(module)

        if transform_instances:
            all_transforms += transform_instances

    return all_transforms
