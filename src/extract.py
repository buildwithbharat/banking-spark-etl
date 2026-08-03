from pyspark.sql import DataFrame, SparkSession


def read_dataset(
    spark: SparkSession,
    path: str,
    file_format: str = "parquet",
    **options,
) -> DataFrame:
    """
    Read a dataset.
    """

    return (
        spark.read
        .options(**options)
        .format(file_format)
        .load(path)
    )


def read_datasets(spark: SparkSession, config: dict) -> dict:
    """
    Read all datasets defined in the configuration.
    """

    dataframes = {}

    for dataset_name, dataset in config["datasets"].items():

        dataframes[dataset_name] = read_dataset(
            spark=spark,
            path=dataset["path"],
            file_format=dataset["format"],
        )

    return dataframes