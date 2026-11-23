# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ##### Notebook Name : Bronze_Vendor_Discovery_NB
# MAGIC - **Created at** (dd/mm/yyyy) : 04/11/2026
# MAGIC - **Description** : Raw (bronze) ingestion of the AVL, ME2N, ME5A and ZMM30 CSVs and the Info Record Excel file from the Volume. All columns are kept as STRING with snake_case names; no row filters (those belong to silver).
# MAGIC - **Modified at** (dd/mm/yyyy) : 16/11/2026
# MAGIC - **Modified** : Cleanup - fixed duplicate snake() definition, fully qualified table names, explicit STRING schema for the Excel load, parameterised metadata upsert, split load / metadata / checks cells.

# COMMAND ----------

# MAGIC %pip install openpyxl -q

# COMMAND ----------

import re
from datetime import datetime

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

dbutils.widgets.text("catalog", "rubicon_dev", "Target catalog (rubicon_dev / rubicon_qa / rubicon_prod)")
CATALOG = dbutils.widgets.get("catalog")
dbutils.widgets.text("source_catalog", "rubicon_poc_workspace", "Catalog holding the raw landing Volume and samwed spec tables")
SOURCE_CATALOG = dbutils.widgets.get("source_catalog")
SCHEMA  = "bronze"
VOLUME_CSV = f"/Volumes/{SOURCE_CATALOG}/samwed/samwed_pdfs/postgre_tables"
VOLUME_PDF = f"/Volumes/{SOURCE_CATALOG}/samwed/samwed_pdfs/pdfs/"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")  

# COMMAND ----------

# All columns stay STRING on purpose so codes keep their exact format.
SOURCES = {"avl_october": "avl", "ME2N with Rate": "me2n", "me5a": "me5a", "zmm30": "zmm30"}   # csv file name -> bronze table

def snake(c):
    """'Purchasing Info Rec.' -> 'purchasing_info_rec'"""
    return re.sub(r"[^0-9a-zA-Z]+", "_", str(c).strip()).strip("_").lower()

def load_csv(file_name, table_name):
    raw = (spark.read
           .option("header", True).option("multiLine", True)
           .option("escape", '"').option("inferSchema", False)
           .csv(f"{VOLUME_CSV}/{file_name}.csv"))
    cols = [snake(c) for c in raw.columns]          # single schema lookup per file
    (raw.toDF(*cols).write.mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.{table_name}"))
    return cols

for file_name, table_name in SOURCES.items():
    cols = load_csv(file_name, table_name)
    n = spark.table(f"{CATALOG}.{SCHEMA}.{table_name}").count()   # Delta count served from metadata
    print(f"{table_name}: {n} rows | {cols}")

# COMMAND ----------

# Info Record list (sheet "Data"). Spark has no native xlsx reader, so pandas reads this single small file (needs openpyxl, cell 2).
# Repeated headers (Scale Quantity / Amount x5) arrive as 'Scale Quantity.1' -> scale_quantity_1, amount_1 ...
INFO_FILE = "Info Record list.xlsx"
INFO_TBL  = f"{CATALOG}.{SCHEMA}.info_record"

_t0 = datetime.now()
pdf = pd.read_excel(f"{VOLUME_CSV}/{INFO_FILE}", sheet_name="Data", dtype=str)
pdf.columns = [snake(c) for c in pdf.columns]
pdf = pdf.astype(object).where(pd.notnull(pdf), None)                              # NaN -> NULL
schema = StructType([StructField(c, StringType(), True) for c in pdf.columns])     # explicit STRING schema, no inference

(spark.createDataFrame(pdf, schema=schema)
    .withColumn("_loaded_at", F.current_timestamp())
    .withColumn("_source_file", F.lit(INFO_FILE))
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(INFO_TBL))
print(INFO_TBL, len(pdf), "rows")                                                  # expect 8469

# COMMAND ----------

# Register / refresh the info_record run in the common metadata table (one row per catalog.schema.table).
METADATA_TBL = f"{CATALOG}.metadata.pipeline_metadata"
meta_args = {"cat": CATALOG, "sch": SCHEMA, "tbl": "info_record", "start": _t0.strftime("%Y-%m-%d %H:%M:%S")}
key = "catalog_name = :cat AND schema_name = :sch AND table_name = :tbl"

if not spark.sql(f"SELECT 1 FROM {METADATA_TBL} WHERE {key} LIMIT 1", args=meta_args).isEmpty():
    spark.sql(f"""
        UPDATE {METADATA_TBL}
        SET pipeline_start_time = CAST(:start AS TIMESTAMP), pipeline_end_time = current_timestamp(),
            last_run_status = 'Succeeded', last_modified_date = current_timestamp(), last_modified_by = current_user()
        WHERE {key}""", args=meta_args)
else:
    spark.sql(f"""
        INSERT INTO {METADATA_TBL}
        SELECT COALESCE(MAX(table_id), 0) + 1, :tbl, :sch, :cat, 'daily_once', 'full', 'NA', 'NA',
               CAST(:start AS TIMESTAMP), current_timestamp(), 'bronze', 'Succeeded',
               current_timestamp(), current_user(), current_timestamp(), current_user()
        FROM {METADATA_TBL}""", args=meta_args)

display(spark.sql(f"SELECT * FROM {METADATA_TBL} WHERE {key}", args=meta_args))

# COMMAND ----------

# Sanity checks on info_record.
# Silver reads: parent_material, material, supplier, plant, net_price, currency, price_unit, order_price_unit, valid_from, valid_to,
# planned_deliv_time, standard_po_quantity, overdeliv_tolerance, source_list_y_n, purchasing_group, deletion_indicator
SILVER_COLS = ["parent_material", "material", "supplier", "plant", "net_price", "currency", "price_unit", "order_price_unit",
               "valid_from", "valid_to", "planned_deliv_time", "standard_po_quantity", "overdeliv_tolerance",
               "source_list_y_n", "purchasing_group", "deletion_indicator"]
missing = sorted(set(SILVER_COLS) - set(spark.table(INFO_TBL).columns))
print("Columns needed by silver but missing:", missing or "none")

display(spark.sql(f"SELECT COUNT(*) AS rows, COUNT(DISTINCT purchasing_info_rec) AS info_records FROM {INFO_TBL}"))
display(spark.sql(f"SELECT valid_from, valid_to, net_price, price_unit FROM {INFO_TBL} LIMIT 5"))   # date text or Excel serial number?
