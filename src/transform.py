from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def transform_customers(df: DataFrame) -> DataFrame:
    return (
        df.dropDuplicates(["customer_id"])
        .withColumn("first_name", F.trim("first_name"))
        .withColumn("last_name", F.trim("last_name"))
    )

def transform_accounts(df: DataFrame) -> DataFrame:
    return (
        df.dropDuplicates(["account_id"])
        .withColumn("current_balance", F.col("current_balance").cast("double"))
    )

def transform_transactions(df: DataFrame) -> DataFrame:
    return (
        df.dropDuplicates(["transaction_id"])
        .withColumn("amount", F.col("amount").cast("double"))
    )

def transform_loans(df: DataFrame) -> DataFrame:
    return (
        df.dropDuplicates(["loan_id"])
        .withColumn("loan_amount", F.col("loan_amount").cast("double"))
        .withColumn(
            "outstanding_balance",
            F.col("outstanding_balance").cast("double")
        )
    )