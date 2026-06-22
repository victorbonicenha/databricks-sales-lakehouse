# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Data Quality Checks
# MAGIC
# MAGIC Checks simples de qualidade para validar as principais tabelas do pipeline.

# COMMAND ----------

from datetime import datetime
from pyspark.sql import Row

# COMMAND ----------

dbutils.widgets.text("database_name", "sales_lakehouse")
database_name = dbutils.widgets.get("database_name")

spark.sql(f"USE {database_name}")

# COMMAND ----------

def sql_value(query: str):
    return spark.sql(query).collect()[0][0]

checked_at = datetime.utcnow().isoformat()
checks = []

# COMMAND ----------

bronze_orders_count = sql_value(f"SELECT COUNT(*) FROM {database_name}.bronze_orders")
checks.append(
    Row(
        check_name="bronze_orders_has_rows",
        table_name="bronze_orders",
        status="PASS" if bronze_orders_count > 0 else "FAIL",
        observed_value=str(bronze_orders_count),
        expected_rule="count > 0",
        checked_at=checked_at,
    )
)

silver_orders_duplicates = sql_value(
    f"""
    SELECT COUNT(*)
    FROM (
        SELECT order_id, COUNT(*) AS qtd
        FROM {database_name}.silver_orders
        GROUP BY order_id
        HAVING COUNT(*) > 1
    )
    """
)
checks.append(
    Row(
        check_name="silver_orders_no_duplicate_order_id",
        table_name="silver_orders",
        status="PASS" if silver_orders_duplicates == 0 else "FAIL",
        observed_value=str(silver_orders_duplicates),
        expected_rule="duplicate order_id = 0",
        checked_at=checked_at,
    )
)

silver_orders_invalid_quantity = sql_value(
    f"SELECT COUNT(*) FROM {database_name}.silver_orders WHERE quantity <= 0 OR quantity IS NULL"
)
checks.append(
    Row(
        check_name="silver_orders_valid_quantity",
        table_name="silver_orders",
        status="PASS" if silver_orders_invalid_quantity == 0 else "FAIL",
        observed_value=str(silver_orders_invalid_quantity),
        expected_rule="quantity > 0",
        checked_at=checked_at,
    )
)

silver_order_details_missing_keys = sql_value(
    f"""
    SELECT COUNT(*)
    FROM {database_name}.silver_order_details
    WHERE customer_id IS NULL OR product_id IS NULL OR order_id IS NULL
    """
)
checks.append(
    Row(
        check_name="silver_order_details_required_keys",
        table_name="silver_order_details",
        status="PASS" if silver_order_details_missing_keys == 0 else "FAIL",
        observed_value=str(silver_order_details_missing_keys),
        expected_rule="required keys cannot be null",
        checked_at=checked_at,
    )
)

payment_difference_count = sql_value(
    f"""
    SELECT COUNT(*)
    FROM {database_name}.silver_order_details
    WHERE payment_amount_difference IS NOT NULL
      AND ABS(payment_amount_difference) > 0.01
    """
)
checks.append(
    Row(
        check_name="payment_amount_matches_order_total",
        table_name="silver_order_details",
        status="WARN" if payment_difference_count > 0 else "PASS",
        observed_value=str(payment_difference_count),
        expected_rule="payment amount should match order total",
        checked_at=checked_at,
    )
)

gold_daily_sales_count = sql_value(f"SELECT COUNT(*) FROM {database_name}.gold_daily_sales")
checks.append(
    Row(
        check_name="gold_daily_sales_has_rows",
        table_name="gold_daily_sales",
        status="PASS" if gold_daily_sales_count > 0 else "FAIL",
        observed_value=str(gold_daily_sales_count),
        expected_rule="count > 0",
        checked_at=checked_at,
    )
)

# COMMAND ----------

dq_results = spark.createDataFrame(checks)

dq_results.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{database_name}.dq_results")

# COMMAND ----------

display(dq_results)
