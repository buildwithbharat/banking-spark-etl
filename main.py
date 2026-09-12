from src.config import load_config
from src.extract import read_datasets
from src.session import create_spark_session
from src.load import write_dataset
from src.transform import (
    customer_transactions,
    transform_customers,
    transform_accounts,
    transform_transactions,
    transform_loans,
    create_customer_transaction_summary,
    rank_customer_transactions,)

def main():

    config = load_config()
    spark = create_spark_session(config['spark']['app_name'])

    dataframes = read_datasets(spark, config)

    customers = transform_customers(dataframes['customers'])
    accounts = transform_accounts(dataframes['accounts'])
    transactions = transform_transactions(dataframes['transactions'])
    loans = transform_loans(dataframes['loans'])

    print(f'Customers: {customers.count()}')
    print(f'Accounts: {accounts.count()}')
    print(f'Transactions: {transactions.count()}')
    print(f'Loans: {loans.count()}')

    customer_transaction_summary = create_customer_transaction_summary(customers, accounts, transactions)

    print(f'Customer transaction summary: {customer_transaction_summary.count()}')
    customer_transaction_summary.show(10, truncate=False)

    customer_transactions_df = customer_transactions(accounts, transactions)

    ranked_customer_transactions = rank_customer_transactions(customer_transactions_df)

    print(f'Ranked customer transactions: {ranked_customer_transactions.count()}')
    ranked_customer_transactions.show(25, truncate=False)

    write_dataset(
    customer_transaction_summary,
    config['outputs']['customer_transaction_summary']['path'],
    config['outputs']['customer_transaction_summary']['format'])

    write_dataset(
    ranked_customer_transactions,
    config['outputs']['ranked_customer_transactions']['path'],
    config['outputs']['ranked_customer_transactions']['format'])

    spark.stop()

if __name__ == '__main__':
    main()