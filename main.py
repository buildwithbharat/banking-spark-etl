from src.config import load_config
from src.extract import read_dataset
from src.session import create_spark_session


def main():
    config = load_config()

    spark = create_spark_session(
        config["spark"]["app_name"]
    )

    customers = read_dataset(
    spark=spark,
    path=config["paths"]["customers"]["path"],
    file_format=config["paths"]["customers"]["format"],
)

    print("Customer Schema")
    customers.printSchema()

    print(f"Customer Rows: {customers.count()}")

    print("Sample Data")
    customers.show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()