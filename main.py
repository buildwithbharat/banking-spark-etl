from src.config import load_config
from src.extract import read_datasets
from src.session import create_spark_session
from src.transform import (
    transform_customers,
    transform_accounts,
    transform_transactions,
    transform_loans,
)


def main():

    config = load_config()

    spark = create_spark_session(
        config["spark"]["app_name"]
    )

    dataframes = read_datasets(spark, config)

    customers = transform_customers(dataframes["customers"])
    accounts = transform_accounts(dataframes["accounts"])
    transactions = transform_transactions(dataframes["transactions"])
    loans = transform_loans(dataframes["loans"])

    print(f"Customers: {customers.count()}")
    print(f"Accounts: {accounts.count()}")
    print(f"Transactions: {transactions.count()}")
    print(f"Loans: {loans.count()}")

    spark.stop()


if __name__ == "__main__":
    main()