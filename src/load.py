from pyspark.sql import DataFrame


def write_dataset(dataframe: DataFrame, path: str, file_format: str = 'parquet') -> None:
    dataframe.write.mode('overwrite').format(file_format).save(path)