# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup
# MAGIC
# MAGIC Este notebook prepara o schema/database do projeto e centraliza os caminhos usados pelos demais notebooks.

# COMMAND ----------

# Ajuste esse caminho para o local onde você subiu a pasta data/raw no Databricks.
# Exemplos:
# /Volumes/workspace/default/sales_lakehouse/raw
# dbfs:/FileStore/sales_lakehouse/raw

dbutils.widgets.text("raw_base_path", "/Volumes/workspace/default/sales_lakehouse/raw")
dbutils.widgets.text("database_name", "sales_lakehouse")

raw_base_path = dbutils.widgets.get("raw_base_path")
database_name = dbutils.widgets.get("database_name")

print(f"raw_base_path = {raw_base_path}")
print(f"database_name = {database_name}")

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
spark.sql(f"USE {database_name}")

print(f"Database/schema preparado: {database_name}")

# COMMAND ----------

# Teste rápido para verificar se os arquivos existem no caminho informado.
# Se der erro aqui, ajustar o widget raw_base_path.

try:
    display(dbutils.fs.ls(raw_base_path))
except Exception as error:
    print("Não foi possível listar o caminho informado.")
    print("Ajuste raw_base_path para o local onde você subiu data/raw.")
    raise error
