from src.session import create_spark_session


def main():

    spark = create_spark_session()

    print("=" * 60)
    print(f"Spark Version : {spark.version}")
    print(f"Application   : {spark.sparkContext.appName}")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()