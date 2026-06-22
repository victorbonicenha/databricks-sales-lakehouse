# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver Transformations
# MAGIC
# MAGIC Limpeza, padronização, tipagem, deduplicação e separação de registros inválidos.

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text("database_name", "sales_lakehouse")
database_name = dbutils.widgets.get("database_name")

spark.sql(f"USE {database_name}")

# COMMAND ----------

customers_bronze = spark.table(f"{database_name}.bronze_customers")
products_bronze = spark.table(f"{database_name}.bronze_products")
orders_bronze = spark.table(f"{database_name}.bronze_orders")
payments_bronze = spark.table(f"{database_name}.bronze_payments")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Customers

# COMMAND ----------

customers_typed = (
    customers_bronze
    .withColumn("customer_id", F.col("customer_id").cast("long"))
    .withColumn("customer_name", F.initcap(F.trim(F.col("customer_name"))))
    .withColumn("email", F.lower(F.trim(F.col("email"))))
    .withColumn("country", F.upper(F.trim(F.col("country"))))
    .withColumn("signup_date", F.to_date(F.col("signup_date")))
)

customers_valid = customers_typed.filter(
    (F.col("customer_id").isNotNull())
    & (F.col("email").isNotNull())
    & (F.col("email") != "")
    & (F.col("signup_date").isNotNull())
)

customer_window = Window.partitionBy("customer_id").orderBy(F.col("ingestion_timestamp").desc())

silver_customers = (
    customers_valid
    .withColumn("row_number", F.row_number().over(customer_window))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Products

# COMMAND ----------

products_typed = (
    products_bronze
    .withColumn("product_id", F.col("product_id").cast("long"))
    .withColumn("product_name", F.initcap(F.trim(F.col("product_name"))))
    .withColumn("category", F.initcap(F.trim(F.col("category"))))
    .withColumn("unit_price", F.col("unit_price").cast("decimal(12,2)"))
)

silver_rejected_products = (
    products_typed
    .withColumn(
        "rejection_reason",
        F.concat_ws(
            "; ",
            F.when(F.col("product_id").isNull(), F.lit("product_id nulo ou inválido")),
            F.when(F.col("product_name").isNull() | (F.col("product_name") == ""), F.lit("product_name vazio")),
            F.when(F.col("category").isNull() | (F.col("category") == ""), F.lit("category vazia")),
            F.when(F.col("unit_price").isNull() | (F.col("unit_price") <= 0), F.lit("unit_price inválido")),
        )
    )
    .filter(F.col("rejection_reason") != "")
)

silver_products = (
    products_typed
    .join(silver_rejected_products.select("product_id"), on="product_id", how="left_anti")
    .dropDuplicates(["product_id"])
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Orders

# COMMAND ----------

orders_typed = (
    orders_bronze
    .withColumn("order_id", F.col("order_id").cast("long"))
    .withColumn("customer_id", F.col("customer_id").cast("long"))
    .withColumn("product_id", F.col("product_id").cast("long"))
    .withColumn("order_date", F.to_date(F.col("order_date")))
    .withColumn("status_raw", F.lower(F.trim(F.col("status"))))
    .withColumn(
        "order_status",
        F.when(F.col("status_raw").isin("completed", "complete"), F.lit("completed"))
        .when(F.col("status_raw").isin("cancelled", "canceled"), F.lit("cancelled"))
        .when(F.col("status_raw") == "pending", F.lit("pending"))
        .when(F.col("status_raw") == "shipped", F.lit("shipped"))
        .otherwise(F.lit(None))
    )
    .withColumn("quantity", F.col("quantity").cast("int"))
    .withColumn("unit_price", F.col("unit_price").cast("decimal(12,2)"))
    .withColumn("order_total", F.round(F.col("quantity") * F.col("unit_price"), 2).cast("decimal(12,2)"))
    .drop("status_raw")
)

orders_invalid_by_rule = (
    orders_typed
    .withColumn(
        "rejection_reason",
        F.concat_ws(
            "; ",
            F.when(F.col("order_id").isNull(), F.lit("order_id nulo ou inválido")),
            F.when(F.col("customer_id").isNull(), F.lit("customer_id nulo ou inválido")),
            F.when(F.col("product_id").isNull(), F.lit("product_id nulo ou inválido")),
            F.when(F.col("order_date").isNull(), F.lit("order_date inválida")),
            F.when(F.col("order_status").isNull(), F.lit("status inválido")),
            F.when(F.col("quantity").isNull() | (F.col("quantity") <= 0), F.lit("quantity inválida")),
            F.when(F.col("unit_price").isNull() | (F.col("unit_price") < 0), F.lit("unit_price inválido")),
        )
    )
    .filter(F.col("rejection_reason") != "")
)

orders_valid_by_rule = (
    orders_typed
    .join(orders_invalid_by_rule.select("order_id"), on="order_id", how="left_anti")
    .dropDuplicates(["order_id"])
)

orders_fk_checked = (
    orders_valid_by_rule.alias("o")
    .join(silver_customers.select(F.col("customer_id").alias("valid_customer_id")), F.col("o.customer_id") == F.col("valid_customer_id"), "left")
    .join(silver_products.select(F.col("product_id").alias("valid_product_id")), F.col("o.product_id") == F.col("valid_product_id"), "left")
)

orders_invalid_by_fk = (
    orders_fk_checked
    .withColumn(
        "rejection_reason",
        F.concat_ws(
            "; ",
            F.when(F.col("valid_customer_id").isNull(), F.lit("customer_id inexistente na Silver")),
            F.when(F.col("valid_product_id").isNull(), F.lit("product_id inexistente na Silver")),
        )
    )
    .filter(F.col("rejection_reason") != "")
    .drop("valid_customer_id", "valid_product_id")
)

silver_orders = (
    orders_fk_checked
    .filter(F.col("valid_customer_id").isNotNull() & F.col("valid_product_id").isNotNull())
    .drop("valid_customer_id", "valid_product_id")
)

silver_rejected_orders = orders_invalid_by_rule.unionByName(orders_invalid_by_fk, allowMissingColumns=True)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Payments

# COMMAND ----------

payments_typed = (
    payments_bronze
    .withColumn("order_id", F.col("order_id").cast("long"))
    .withColumn("payment_id", F.trim(F.col("payment_id")))
    .withColumn("payment_method", F.lower(F.trim(F.col("payment_method"))))
    .withColumn("payment_status", F.lower(F.trim(F.col("payment_status"))))
    .withColumn("amount", F.col("amount").cast("decimal(12,2)"))
    .withColumn("paid_at", F.to_timestamp(F.col("paid_at")))
)

silver_payments = (
    payments_typed
    .filter(
        (F.col("payment_id").isNotNull())
        & (F.col("order_id").isNotNull())
        & (F.col("amount").isNotNull())
        & (F.col("amount") >= 0)
        & (F.col("payment_status").isin("approved", "pending", "refused"))
    )
    .dropDuplicates(["payment_id"])
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Order Details

# COMMAND ----------

silver_order_details = (
    silver_orders.alias("o")
    .join(silver_customers.alias("c"), on="customer_id", how="left")
    .join(silver_products.alias("p"), on="product_id", how="left")
    .join(silver_payments.alias("pay"), on="order_id", how="left")
    .select(
        "order_id",
        "customer_id",
        "customer_name",
        "email",
        "country",
        "product_id",
        "product_name",
        "category",
        "order_date",
        "order_status",
        "quantity",
        F.col("o.unit_price").alias("order_unit_price"),
        "order_total",
        "payment_id",
        "payment_method",
        "payment_status",
        F.col("amount").alias("payment_amount"),
        "paid_at",
        F.round(F.col("amount") - F.col("order_total"), 2).alias("payment_amount_difference"),
        F.col("o.ingestion_timestamp").alias("order_ingestion_timestamp"),
        F.col("o.source_file").alias("order_source_file"),
    )
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Save Silver Tables

# COMMAND ----------

silver_customers.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{database_name}.silver_customers")
silver_products.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{database_name}.silver_products")
silver_orders.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{database_name}.silver_orders")
silver_payments.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{database_name}.silver_payments")
silver_order_details.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{database_name}.silver_order_details")
silver_rejected_orders.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{database_name}.silver_rejected_orders")
silver_rejected_products.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{database_name}.silver_rejected_products")

# COMMAND ----------

for table_name in [
    "silver_customers",
    "silver_products",
    "silver_orders",
    "silver_payments",
    "silver_order_details",
    "silver_rejected_orders",
    "silver_rejected_products",
]:
    count_rows = spark.table(f"{database_name}.{table_name}").count()
    print(f"{table_name}: {count_rows} linhas")

# COMMAND ----------

display(spark.table(f"{database_name}.silver_order_details").limit(20))
