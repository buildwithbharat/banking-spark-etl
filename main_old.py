from src.config import load_config
from src.extract import read_datasets
from src.session import create_spark_session
from src.transform import transform_customers


def main():

    config = load_config()

    spark = create_spark_session(
        config["spark"]["app_name"]
    )

    dataframes = read_datasets(spark, config)

    customers = transform_customers(dataframes["customers"])

    print(f"Customers after transformation: {customers.count()}")

    for name, dataframe in dataframes.items():
        print(name)
        dataframe.show(3, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()