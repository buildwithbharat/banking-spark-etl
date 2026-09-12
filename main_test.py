from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql import functions as F

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
    rank_customer_transactions,
    customer_transaction_summary_sql,)

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
    print(f'Loans: {loans.count()}')

    import time

    transactions.cache()

    start_time = time.perf_counter()
    transactions.count()
    first_count_time = time.perf_counter() - start_time

    start_time = time.perf_counter()
    transactions.count()
    second_count_time = time.perf_counter() - start_time
   
    print(f'First count time: {first_count_time:.2f} seconds')
    print(f'Second count time: {second_count_time:.2f} seconds')

    #print(f'Transaction Partitions: {transactions.rdd.getNumPartitions()}')

    #repartitioned_transactions = transactions.repartition(20)
    #print(f'Repartitioned transactions: {repartitioned_transactions.rdd.getNumPartitions()}')

    #repartitioned_transactions.explain()

    #coalesced_transactions = transactions.coalesce(5)
    #print(f'Coalesced transactions: {coalesced_transactions.rdd.getNumPartitions()}')

    #spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '-1')

    #broadcast_join = transactions.join(F.broadcast(accounts),'account_id','inner')
    #normal_join = transactions.join(accounts,'account_id','inner')

    #print('Normal Join:')
    #normal_join.explain()

    #print('Broadcast Join:')
    #broadcast_join.explain()

    #customer_transaction_summary = create_customer_transaction_summary(customers, accounts, transactions)

    #print(f'Customer transaction summary: {customer_transaction_summary.count()}')
    #customer_transaction_summary.show(10, truncate=False)

    #customer_transactions_df = customer_transactions(accounts, transactions)

    #ranked_customer_transactions = rank_customer_transactions(customer_transactions_df)

    #print(f'Ranked customer transactions: {ranked_customer_transactions.count()}')
    #ranked_customer_transactions.show(10, truncate=False)

    #cust_tran_summary_sql = customer_transaction_summary_sql(customers,accounts,transactions)

    #print(f'Customer transaction summary SQL: {cust_tran_summary_sql.count()}')
    #cust_tran_summary_sql.show(10, truncate=False)

    #write_dataset(
    #customer_transaction_summary,
    #config['outputs']['customer_transaction_summary']['path'],
    #config['outputs']['customer_transaction_summary']['format'])

    #write_dataset(
    #ranked_customer_transactions,
    #config['outputs']['ranked_customer_transactions']['path'],
    #config['outputs']['ranked_customer_transactions']['format'])

    spark.stop()

if __name__ == '__main__':
    main()