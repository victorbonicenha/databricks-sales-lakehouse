# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold Analytics
# MAGIC
# MAGIC Criação das tabelas analíticas para consumo em SQL/Dashboard.

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("database_name", "sales_lakehouse")
database_name = dbutils.widgets.get("database_name")

spark.sql(f"USE {database_name}")

# COMMAND ----------

order_details = spark.table(f"{database_name}.silver_order_details")

# Considerando como venda válida pedidos completed/shipped com pagamento aprovado.
sales_base = order_details.filter(
    (F.col("order_status").isin("completed", "shipped"))
    & (F.col("payment_status") == "approved")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Faturamento diário

# COMMAND ----------

gold_daily_sales = (
    sales_base
    .groupBy("order_date")
    .agg(
        F.countDistinct("order_id").alias("orders_count"),
        F.sum("quantity").alias("items_sold"),
        F.round(F.sum("order_total"), 2).alias("gross_revenue"),
        F.round(F.avg("order_total"), 2).alias("avg_ticket"),
    )
    .orderBy("order_date")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Vendas por categoria

# COMMAND ----------

gold_sales_by_category = (
    sales_base
    .groupBy("category")
    .agg(
        F.countDistinct("order_id").alias("orders_count"),
        F.sum("quantity").alias("items_sold"),
        F.round(F.sum("order_total"), 2).alias("gross_revenue"),
        F.round(F.avg("order_total"), 2).alias("avg_ticket"),
    )
    .orderBy(F.col("gross_revenue").desc())
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Top clientes

# COMMAND ----------

gold_top_customers = (
    sales_base
    .groupBy("customer_id", "customer_name", "email", "country")
    .agg(
        F.countDistinct("order_id").alias("orders_count"),
        F.sum("quantity").alias("items_bought"),
        F.round(F.sum("order_total"), 2).alias("total_spent"),
        F.max("order_date").alias("last_order_date"),
    )
    .orderBy(F.col("total_spent").desc())
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Resumo de pagamentos

# COMMAND ----------

gold_payment_summary = (
    order_details
    .groupBy("payment_method", "payment_status")
    .agg(
        F.countDistinct("payment_id").alias("payments_count"),
        F.round(F.sum("payment_amount"), 2).alias("payment_amount_total"),
        F.round(F.avg("payment_amount"), 2).alias("avg_payment_amount"),
    )
    .orderBy("payment_method", "payment_status")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Resumo de status dos pedidos

# COMMAND ----------

gold_order_status_summary = (
    order_details
    .groupBy("order_status")
    .agg(
        F.countDistinct("order_id").alias("orders_count"),
        F.round(F.sum("order_total"), 2).alias("order_amount_total"),
    )
    .orderBy(F.col("orders_count").desc())
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Save Gold Tables

# COMMAND ----------

gold_daily_sales.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{database_name}.gold_daily_sales")
gold_sales_by_category.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{database_name}.gold_sales_by_category")
gold_top_customers.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{database_name}.gold_top_customers")
gold_payment_summary.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{database_name}.gold_payment_summary")
gold_order_status_summary.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{database_name}.gold_order_status_summary")

# COMMAND ----------

for table_name in [
    "gold_daily_sales",
    "gold_sales_by_category",
    "gold_top_customers",
    "gold_payment_summary",
    "gold_order_status_summary",
]:
    count_rows = spark.table(f"{database_name}.{table_name}").count()
    print(f"{table_name}: {count_rows} linhas")

# COMMAND ----------

display(spark.table(f"{database_name}.gold_daily_sales"))

# COMMAND ----------

display(spark.table(f"{database_name}.gold_sales_by_category"))
