from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "BankingSparkETL") -> SparkSession:
    """
    Create and return a SparkSession.
    """

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    
    return spark