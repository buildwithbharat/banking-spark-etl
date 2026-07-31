from pyspark.sql import DataFrame, SparkSession


def read_dataset(
    spark: SparkSession,
    path: str,
    file_format: str = "parquet",
    **options
) -> DataFrame:
    """
    Read a dataset in the specified format.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    path : str
        Path to the dataset.
    file_format : str
        Dataset format (parquet, csv, json, etc.).
    options : dict
        Additional Spark read options.
    """
    return (
        spark.read
        .options(**options)
        .format(file_format)
        .load(path)
    )