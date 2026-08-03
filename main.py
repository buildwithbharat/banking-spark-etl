from src.config import load_config
from src.extract import read_datasets
from src.session import create_spark_session


def main():

    config = load_config()

    spark = create_spark_session(
        config["spark"]["app_name"]
    )

    dataframes = read_datasets(spark, config)

    for name, dataframe in dataframes.items():

        print(f"\n{name.upper()}")
        print("-" * 40)

        print(f"Rows: {dataframe.count()}")

    spark.stop()


if __name__ == "__main__":
    main()