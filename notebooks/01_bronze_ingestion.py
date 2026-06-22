# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze Ingestion
# MAGIC
# MAGIC Ingestão dos arquivos crus para tabelas Delta na camada Bronze.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("raw_base_path", "/Volumes/workspace/default/sales_lakehouse/raw")
dbutils.widgets.text("database_name", "sales_lakehouse")

raw_base_path = dbutils.widgets.get("raw_base_path")
database_name = dbutils.widgets.get("database_name")

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
spark.sql(f"USE {database_name}")

print(f"Lendo arquivos de: {raw_base_path}")

# COMMAND ----------

def add_ingestion_metadata(df):
    return (
        df
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
    )

# COMMAND ----------

customers_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(f"{raw_base_path}/customers/*.csv")
)

products_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(f"{raw_base_path}/products/*.csv")
)

orders_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(f"{raw_base_path}/orders/*.csv")
)

payments_df = (
    spark.read
    .json(f"{raw_base_path}/payments/*.json")
)

# COMMAND ----------

bronze_customers = add_ingestion_metadata(customers_df)
bronze_products = add_ingestion_metadata(products_df)
bronze_orders = add_ingestion_metadata(orders_df)
bronze_payments = add_ingestion_metadata(payments_df)

# COMMAND ----------

bronze_customers.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{database_name}.bronze_customers")
bronze_products.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{database_name}.bronze_products")
bronze_orders.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{database_name}.bronze_orders")
bronze_payments.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{database_name}.bronze_payments")

# COMMAND ----------

for table_name in ["bronze_customers", "bronze_products", "bronze_orders", "bronze_payments"]:
    count_rows = spark.table(f"{database_name}.{table_name}").count()
    print(f"{table_name}: {count_rows} linhas")

# COMMAND ----------

display(spark.table(f"{database_name}.bronze_orders").limit(10))
