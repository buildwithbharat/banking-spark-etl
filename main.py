from src.extract import read_parquet
from src.session import create_spark_session


def main():

    spark = create_spark_session()

    customers = read_parquet(spark, "data/input/customers.parquet")

    print("\nSchema\n")

    customers.printSchema()

    print("\nSample Data\n")

    customers.show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()