from src.config import load_config
from src.extract import read_dataset
from src.session import create_spark_session


def main():

    config = load_config()

    spark = create_spark_session(
        config["spark"]["app_name"]
    )

    employees = read_dataset(
        spark=spark,
        path=config["datasets"]["employees"]["path"],
        file_format=config["datasets"]["employees"]["format"],
    )

    print(f"Rows    : {employees.count()}")
    print(f"Columns : {len(employees.columns)}")

    employees.printSchema()
    employees.show(3, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()