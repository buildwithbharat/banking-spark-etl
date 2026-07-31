from pyspark.sql import DataFrame
from pyspark.sql import SparkSession


def read_parquet(spark: SparkSession, path: str) -> DataFrame:
    """
    Read a Parquet dataset.
    """

    return spark.read.parquet(path)