# Databricks notebook source
# MAGIC %md
# MAGIC ##### 00_setup - creates the schemas and the common metadata table in the target catalog (safe to re-run)

# COMMAND ----------

dbutils.widgets.text("catalog", "rubicon_dev", "Target catalog (rubicon_dev / rubicon_qa / rubicon_prod)")
CATALOG = dbutils.widgets.get("catalog")
assert CATALOG in ("rubicon_dev", "rubicon_qa", "rubicon_prod"), f"Unexpected catalog {CATALOG}"

for schema in ("bronze", "silver", "gold", "metadata"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

spark.sql(f"""CREATE TABLE IF NOT EXISTS {CATALOG}.metadata.pipeline_metadata (
  table_id BIGINT, table_name STRING, schema_name STRING, catalog_name STRING, refresh_frequency STRING,
  load_type STRING, primary_key STRING, merge_key STRING, pipeline_start_time TIMESTAMP, pipeline_end_time TIMESTAMP,
  zone STRING, last_run_status STRING, created_date TIMESTAMP, created_by STRING,
  last_modified_date TIMESTAMP, last_modified_by STRING)""")
print("Ready:", CATALOG)
