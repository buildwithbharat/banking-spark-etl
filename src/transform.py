from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql import functions as F

def transform_customers(customers: DataFrame) -> DataFrame:
    return customers.dropDuplicates(['customer_id']).withColumn('first_name', F.trim('first_name')).withColumn('last_name', F.trim('last_name'))

def transform_accounts(accounts: DataFrame) -> DataFrame:
    return accounts.dropDuplicates(['account_id']).withColumn('current_balance', F.col('current_balance').cast('double'))

def transform_transactions(transactions: DataFrame) -> DataFrame:
    return transactions.dropDuplicates(['transaction_id']).withColumn('amount', F.col('amount').cast('double'))

def transform_loans(loans: DataFrame) -> DataFrame:
    return loans.dropDuplicates(['loan_id']).withColumn('loan_amount', F.col('loan_amount').cast('double')).withColumn('outstanding_balance', F.col('outstanding_balance').cast('double'))

def create_customer_transaction_summary(
    customers: DataFrame,
    accounts: DataFrame,
    transactions: DataFrame,
) -> DataFrame:

    account_transactions = accounts.join(transactions, 'account_id', 'inner')
    customer_transactions = customers.join(account_transactions, 'customer_id', 'inner')

    result_df = customer_transactions.groupBy('customer_id').agg(F.count('transaction_id').alias('total_transactions'),
            F.sum(F.when(F.col('transaction_status') == 'Success', 1).otherwise(0)).alias('successful_transactions'),
            F.sum('amount').alias('total_transaction_amount'),
            F.round(F.avg('amount'),3).alias('average_transaction_amount'),
            F.sum(F.when(F.col('transaction_type') == 'Credit',F.col('amount')).otherwise(0)).alias('total_credit_amount'),
            F.sum(F.when(F.col('transaction_type') == 'Debit',F.col('amount')).otherwise(0)).alias('total_debit_amount'))

    return result_df

def customer_transactions(accounts: DataFrame, transactions: DataFrame,) -> DataFrame:

    return accounts.select('account_id', 'customer_id').join(transactions,'account_id','inner')

def rank_customer_transactions(customer_transactions: DataFrame) -> DataFrame:
    window = Window.partitionBy('customer_id').orderBy(F.col('amount').desc())

    return customer_transactions.withColumn('transaction_rank', F.dense_rank().over(window))

def customer_transaction_summary_sql(customers: DataFrame, accounts: DataFrame, transactions: DataFrame) -> DataFrame:

    customers.createOrReplaceTempView('customers')
    accounts.createOrReplaceTempView('accounts')
    transactions.createOrReplaceTempView('transactions')

    return customers.sparkSession.sql("""SELECT
            c.customer_id,
            COUNT(t.transaction_id) AS total_transactions,
            SUM(CASE WHEN t.transaction_status = 'Success' THEN 1 ELSE 0 END) AS successful_transactions,
            SUM(t.amount) AS total_transaction_amount,
            ROUND(AVG(t.amount), 3) AS average_transaction_amount,
            SUM(CASE WHEN t.transaction_type = 'Credit' THEN t.amount ELSE 0 END) AS total_credit_amount,
            SUM(CASE WHEN t.transaction_type = 'Debit' THEN t.amount ELSE 0 END) AS total_debit_amount
        FROM customers c INNER JOIN accounts a ON c.customer_id = a.customer_id
        INNER JOIN transactions t ON a.account_id = t.account_id
        GROUP BY c.customer_id""")