# Databricks notebook source
# MAGIC %md
# MAGIC ##### Notebook Name : Silver_Vendor_Discovery_NB
# MAGIC - **Created at** (dd/mm/yyyy) : 06/11/2026
# MAGIC - **Modified at** (dd/mm/yyyy) : 12/11/2026
# MAGIC - **Description** : Builds the Silver layer for the PR-to-vendor-to-spec storyline. Transforms bronze `ME5A`, `ME2N`, `ZMM30`, `AVL` and the PDF spec tables into curated tables, **incrementally**.
# MAGIC
# MAGIC **How incremental loading works (every table except the already-incremental spec parser)**
# MAGIC 1. Each table remembers the Delta versions of its source tables (kept as a table property, `source_signature`). If no source or rule changed, the table is skipped.
# MAGIC 2. Otherwise the new snapshot is built and `MERGE`d: only new or changed rows are written (by row hash), and rows that left the source (for example a PR that is no longer pending) are deleted.
# MAGIC 3. If the column list changes, the table is rebuilt once.
# MAGIC 4. Every run writes one row per table to the common table `metadata.pipeline_metadata` (status, start/end time, keys, load type).
# MAGIC 5. Business rules are temp views defined in code (no rule tables); only `silver_spec_parse_rules` stays a table because you edit it by SQL.
# MAGIC
# MAGIC **Stable facts only in silver.** Anything that depends on today's date (TAT elapsed/remaining/status, qualification status, vendor role) is computed in gold, so silver does not change every day.

# COMMAND ----------

from pyspark.sql import functions as F, Window
import re

# COMMAND ----------

dbutils.widgets.text("catalog", "rubicon_dev", "Target catalog (rubicon_dev / rubicon_qa / rubicon_prod)")
CATALOG = dbutils.widgets.get("catalog")
dbutils.widgets.text("source_catalog", "rubicon_poc_workspace", "Catalog holding the raw landing Volume and samwed spec tables")
SOURCE_CATALOG = dbutils.widgets.get("source_catalog")
SCHEMA  = "silver"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

SCOPE_SUBCATEGORIES = ["PM"]     # widen later (e.g. ["PM", "RM (Excipients)", "API"]) - nothing else has to change
FORCE_FULL_LOAD     = False      # True = ignore the "sources unchanged" skip for this run (the merge still only writes changed rows)

# COMMAND ----------

# Incremental-load helper + common metadata table (one row per table, shared by every zone).
from datetime import datetime
import hashlib

METADATA_TBL      = f"{CATALOG}.metadata.pipeline_metadata"
REFRESH_FREQUENCY = "daily_once"
ZONE              = "silver"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.metadata")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {METADATA_TBL} (
  table_id BIGINT, table_name STRING, schema_name STRING, catalog_name STRING, refresh_frequency STRING,
  load_type STRING, primary_key STRING, merge_key STRING, pipeline_start_time TIMESTAMP, pipeline_end_time TIMESTAMP,
  zone STRING, last_run_status STRING, created_date TIMESTAMP, created_by STRING,
  last_modified_date TIMESTAMP, last_modified_by STRING)""")

def meta_upsert(table, load_type, keys, started, status):
    """Insert the table's row the first time, update it on every later run."""
    who = spark.sql("SELECT current_user()").first()[0]
    where = f"catalog_name = '{CATALOG}' AND schema_name = '{SCHEMA}' AND table_name = '{table}'"
    key_txt = ",".join(keys) if keys else "NA"
    start = started.strftime("%Y-%m-%d %H:%M:%S")
    if spark.sql(f"SELECT 1 FROM {METADATA_TBL} WHERE {where}").count() > 0:
        spark.sql(f"""UPDATE {METADATA_TBL} SET refresh_frequency = '{REFRESH_FREQUENCY}', load_type = '{load_type}',
            primary_key = '{key_txt}', merge_key = '{key_txt}', pipeline_start_time = TIMESTAMP'{start}',
            pipeline_end_time = current_timestamp(), last_run_status = '{status}',
            last_modified_date = current_timestamp(), last_modified_by = '{who}' WHERE {where}""")
    else:
        nid = spark.sql(f"SELECT COALESCE(MAX(table_id), 0) + 1 FROM {METADATA_TBL}").first()[0]
        spark.sql(f"""INSERT INTO {METADATA_TBL} VALUES ({nid}, '{table}', '{SCHEMA}', '{CATALOG}', '{REFRESH_FREQUENCY}',
            '{load_type}', '{key_txt}', '{key_txt}', TIMESTAMP'{start}', current_timestamp(), '{ZONE}', '{status}',
            current_timestamp(), '{who}', current_timestamp(), '{who}')""")

def _version(t):
    return spark.sql(f"DESCRIBE HISTORY {t} LIMIT 1").first()["version"]

def _signature(tables, extra=""):
    return "|".join(f"{t}@{_version(t)}" for t in tables) + (f"|{extra}" if extra else "")

def _last_signature(target):
    r = [x for x in spark.sql(f"SHOW TBLPROPERTIES {target}").collect() if x["key"] == "source_signature"]
    return r[0]["value"] if r else None          # remembered on the target table itself, no extra log table

def _merge(build_df, target, keys, sources, extra):
    sig = _signature(sources, extra) if sources else None
    exists = spark.catalog.tableExists(target)
    if sig and exists and not FORCE_FULL_LOAD and _last_signature(target) == sig:
        print(f"{target}: sources unchanged - skipped")
        return "Skipped"
    src = build_df()
    cols = src.columns
    src = src.withColumn("_row_hash", F.xxhash64(*[F.col(c) for c in cols]))
    src = (src.withColumn("_rn", F.row_number().over(Window.partitionBy(*keys).orderBy("_row_hash")))
              .where("_rn = 1").drop("_rn")
              .withColumn("_silver_updated_at", F.current_timestamp()))
    if exists and set(spark.table(target).columns) != set(src.columns):
        print(f"{target}: column list changed - rebuilding once")
        exists = False
    if not exists:
        src.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target)
        ins, upd, dele = src.count(), 0, 0
    else:
        src.createOrReplaceTempView("_merge_src")
        on = " AND ".join(f"t.{k} <=> s.{k}" for k in keys)
        spark.sql(f"""MERGE INTO {target} t USING _merge_src s ON {on}
                      WHEN MATCHED AND t._row_hash <> s._row_hash THEN UPDATE SET *
                      WHEN NOT MATCHED THEN INSERT *
                      WHEN NOT MATCHED BY SOURCE THEN DELETE""")
        m = spark.sql(f"DESCRIBE HISTORY {target} LIMIT 1").first()["operationMetrics"]
        ins, upd, dele = int(m.get("numTargetRowsInserted", 0)), int(m.get("numTargetRowsUpdated", 0)), int(m.get("numTargetRowsDeleted", 0))
    if sig:
        spark.sql(f"ALTER TABLE {target} SET TBLPROPERTIES ('source_signature' = '{sig}')")
    print(f"{target}: inserted {ins}, updated {upd}, deleted {dele}, rows now {spark.table(target).count()}")
    return "Succeeded"

def merge_snapshot(build_df, target, keys, sources=None, extra=""):
    """build_df: function returning the full new snapshot. keys: columns that identify one row.
    sources: tables whose Delta version decides whether anything can have changed (None = always run)."""
    started = datetime.now()
    try:
        status = _merge(build_df, target, keys, sources, extra)
    except Exception:
        meta_upsert(target, "incremental", keys, started, "Failed")
        raise
    meta_upsert(target, "incremental", keys, started, status)

# COMMAND ----------

# Business rules as temp views (no extra tables). To change a rule, edit the lists below and re-run:
# the change is detected automatically (RULES_SIG is part of the load signature), so dim_pr_open_pkg refreshes itself.
# The TAT numbers used by the dashboard are copied into dim_pr_open_pkg, so gold never needs these views.

# (rule_order, category, subcategory, acct_assignment_cat, first_digit, purchasing_group, pg_operator)   None = not tested
# First matching rule (lowest rule_order) wins. Purchasing group is compared with leading zeros removed ('003' -> '3').
CATEGORY_RULES = [
    (1, "Capex", "Capex",           "A",  None, None, None),
    (2, "RM/PM", "API",             None, "1",  "1",  "="),
    (3, "RM/PM", "RM (Excipients)", None, "1",  "1",  "<>"),
    (4, "RM/PM", "PM",              None, "2",  None, None),
    (5, "Opex",  "Opex",            None, None, None, None),
]
# (category, standard_days, risk_max_remaining, missed_bucket1_max_elapsed)
#  Risk when remaining days <= risk_max_remaining; Missed bucket 1 = standard+1 .. missed_bucket1_max_elapsed, bucket 2 = above it.
TAT_RULES = [("Capex", 20, 5, 25), ("Opex", 7, 2, 10), ("RM/PM", 5, 2, 10)]

spark.createDataFrame(CATEGORY_RULES,
    "rule_order int, category string, subcategory string, acct_assignment_cat string, first_digit string, purchasing_group string, pg_operator string"
    ).createOrReplaceTempView("category_rules")
spark.createDataFrame(TAT_RULES,
    "category string, standard_days int, risk_max_remaining int, missed_bucket1_max_elapsed int").createOrReplaceTempView("tat_rules")
RULES_SIG = hashlib.md5(str((CATEGORY_RULES, TAT_RULES, SCOPE_SUBCATEGORIES)).encode()).hexdigest()[:12]

# COMMAND ----------

# dim_pr_open_pkg : one row per pending PR line in scope (REPLACES the old packaging-only cell).
# Client ME5A filters. ZMM30 adds release dates and the PR creator (= buyer); it repeats a PR line once per PO but the
# dates and creator are identical across those rows, so MAX() per line is safe.
# Stored here: stable facts + the TAT start date and the rule numbers. Elapsed / remaining / status are computed daily in gold.
def build_pr():
    scope = ", ".join(f"'{s}'" for s in SCOPE_SUBCATEGORIES)
    return spark.sql(f"""
    WITH z AS (
      SELECT TRIM(purchase_requisition) AS pr, TRIM(item_of_requisition) AS pr_item,
             MAX(TRY_CAST(LEFT(TRIM(pr_l1_release_date),    10) AS DATE)) AS l1_release_date,
             MAX(TRY_CAST(LEFT(TRIM(pr_l2_release_date),    10) AS DATE)) AS l2_release_date,
             MAX(TRY_CAST(LEFT(TRIM(pr_final_release_date), 10) AS DATE)) AS final_release_date,
             MAX(NULLIF(TRIM(pr_created_by), ''))      AS zmm30_created_by,
             MAX(NULLIF(TRIM(pr_created_by_name), '')) AS zmm30_created_by_name
      FROM bronze.zmm30 GROUP BY 1, 2
    ),
    m AS (
      SELECT CONCAT(TRIM(purchase_requisition), '-', TRIM(item_of_requisition)) AS pr_line,
             TRIM(purchase_requisition) AS pr, TRIM(item_of_requisition) AS pr_item, plant,
             TRY_CAST(LEFT(TRIM(requisition_date), 10) AS DATE) AS pr_date,
             NULLIF(TRIM(material), '') AS material, short_text,
             TRY_CAST(quantity_requested AS DOUBLE) AS qty,
             TRY_CAST(quantity_ordered   AS DOUBLE) AS qty_ordered,
             TRY_CAST(quantity_requested AS DOUBLE) - COALESCE(TRY_CAST(quantity_ordered AS DOUBLE), 0) AS qty_pending,
             unit_of_measure AS uom, NULLIF(TRIM(purchase_order), '') AS purchase_order, material_group,
             NULLIF(TRIM(purchasing_group), '') AS purchasing_group,
             NULLIF(TRIM(acct_assignment_cat), '') AS acct_assignment_cat,
             NULLIF(TRIM(created_by), '') AS me5a_created_by
      FROM bronze.me5a
      WHERE NULLIF(TRIM(purchase_requisition), '') IS NOT NULL
        AND LEFT(TRIM(plant), 1) = '1'
        AND COALESCE(TRIM(item_category), '') <> 'U'
        AND COALESCE(TRIM(release_indicator), '') <> 'X'
        AND TRY_CAST(quantity_requested AS DOUBLE) - COALESCE(TRY_CAST(quantity_ordered AS DOUBLE), 0) > 0
    ),
    c AS (
      SELECT m.pr_line, r.category, r.subcategory,
             ROW_NUMBER() OVER (PARTITION BY m.pr_line ORDER BY r.rule_order) AS rn
      FROM m JOIN category_rules r
        ON (r.acct_assignment_cat IS NULL OR r.acct_assignment_cat = m.acct_assignment_cat)
       AND (r.first_digit IS NULL OR r.first_digit = LEFT(m.material, 1))
       AND (r.purchasing_group IS NULL
            OR (r.pg_operator = '='  AND regexp_replace(COALESCE(m.purchasing_group,''), '^0+', '') =  r.purchasing_group)
            OR (r.pg_operator = '<>' AND regexp_replace(COALESCE(m.purchasing_group,''), '^0+', '') <> r.purchasing_group))
    )
    SELECT m.pr_line, m.pr, m.pr_item, m.plant, m.pr_date, m.material, m.short_text, m.qty, m.uom, m.material_group,
           CASE m.material_group WHEN 'PMPR' THEN 'PRIMARY' WHEN 'PMSC' THEN 'SECONDARY' WHEN 'PMTR' THEN 'TERTIARY' END AS pack_category,
           CASE WHEN m.purchase_order IS NULL OR COALESCE(m.qty_ordered, 0) = 0 THEN 'OPEN'
                WHEN m.qty_ordered >= m.qty THEN 'FULLY_ORDERED' ELSE 'PARTIALLY_ORDERED' END AS pr_status,
           m.qty_ordered, m.qty_pending, m.purchase_order, m.purchasing_group,
           c.category, c.subcategory,
           COALESCE(z.zmm30_created_by_name, z.zmm30_created_by, m.me5a_created_by) AS buyer,
           COALESCE(z.zmm30_created_by, m.me5a_created_by)                           AS buyer_id,
           z.l1_release_date, z.l2_release_date, z.final_release_date,
           (z.l1_release_date IS NULL) AS l1_release_missing,
           -- cancelled and reopened (client): a PO was created but its ordered quantity is 0 = cancelled PO, the PR is open again.
           (m.purchase_order IS NOT NULL AND COALESCE(m.qty_ordered, 0) = 0) AS po_cancelled_reopened,
           CASE WHEN m.purchase_order IS NOT NULL AND COALESCE(m.qty_ordered, 0) = 0 THEN m.purchase_order END AS cancelled_pos,
           COALESCE(z.final_release_date, m.pr_date) AS tat_start_date,
           CASE WHEN z.final_release_date IS NOT NULL THEN 'FINAL_RELEASE' ELSE 'REQUISITION' END AS tat_start_source,
           r.standard_days AS tat_standard_days, r.risk_max_remaining AS tat_risk_max_remaining,
           r.missed_bucket1_max_elapsed AS tat_missed_bucket1_max_elapsed
    FROM m
    JOIN c ON c.pr_line = m.pr_line AND c.rn = 1 AND c.subcategory IN ({scope})
    JOIN tat_rules r ON r.category = c.category
    LEFT JOIN z ON z.pr = m.pr AND z.pr_item = m.pr_item
    """)

merge_snapshot(build_pr, "dim_pr_open_pkg", ["pr_line"],
               sources=["bronze.me5a", "bronze.zmm30"], extra="rules=" + RULES_SIG)

# COMMAND ----------

# dim_avl_vendor : approved vendors (AVL) + purchase history by MPN (ME2N).
# ME2N has no vendor column: the vendor is identified through the MPN stored in "SAP Code".
# Client ME2N filters: plant starts with 1, document starts with 4 (also drops 5-series stock transfers), deletion indicator blank.
# Duplicates on PO + item cannot change COUNT(DISTINCT document) or MAX(date), so no separate de-dup is needed.
# Future-dated rows are KEPT (client decision). Rows with no MPN, or SAP code equal to the material code, are dropped.
# AVL: DISTINCT removes exact duplicate rows; supplier_code "30889.0" becomes "30889"; date_suspect flags impossible dates.
# 145 (item, MPN) pairs have different qual_due values across supplier rows; they are kept (open client question).
def make_po_clean():
    """ME2N purchase-order items in scope, with quantity, value, rate and currency.
    Zero-value POs are cancelled POs (client: the PR becomes open again) and are left out, so they never count as a purchase."""
    spark.sql("""
    CREATE OR REPLACE TEMP VIEW po_clean AS
    SELECT NULLIF(TRIM(material), '') AS material, NULLIF(TRIM(sap_code), '') AS mpn,
           TRIM(purchasing_document) AS po, TRY_CAST(TRIM(item) AS INT) AS po_item,
           TRY_CAST(document_date AS DATE) AS document_date,
           TRY_CAST(TRIM(po_quantity) AS DOUBLE) AS po_qty, NULLIF(TRIM(unit_of_measure), '') AS uom,
           TRY_CAST(TRIM(po_value) AS DOUBLE) AS po_value, NULLIF(TRIM(currency), '') AS currency,
           TRY_CAST(TRIM(po_value) AS DOUBLE) / TRY_CAST(TRIM(po_quantity) AS DOUBLE) AS rate   -- PO value / quantity: also right when the price unit differs from the order unit
    FROM bronze.me2n
    WHERE LEFT(TRIM(plant), 1) = '1'
      AND LEFT(TRIM(purchasing_document), 1) = '4'
      AND NULLIF(TRIM(deletion_indicator), '') IS NULL
      AND NULLIF(TRIM(material), '') IS NOT NULL
      AND NULLIF(TRIM(sap_code), '') IS NOT NULL
      AND NULLIF(TRIM(sap_code), '') <> NULLIF(TRIM(material), '')
      AND TRY_CAST(TRIM(po_quantity) AS DOUBLE) > 0
      AND TRY_CAST(TRIM(po_value) AS DOUBLE) > 0""")

def build_avl():
    make_po_clean()
    spark.sql("""
    CREATE OR REPLACE TEMP VIEW po_by_mpn AS
    WITH po AS (SELECT * FROM po_clean),
    z AS (   -- ZMM30 repeats a PO item once per PR line: one row per PO item (creator and dates only; quantity and unit now come from ME2N)
      SELECT TRIM(purchase_order) AS po, TRY_CAST(TRIM(po_item) AS INT) AS po_item,
             MAX(NULLIF(TRIM(po_created_by), ''))                     AS created_by,
             MAX(TRY_CAST(LEFT(TRIM(po_creation_date), 10) AS DATE))  AS created_on,
             MAX(TRY_CAST(LEFT(TRIM(po_delivery_date), 10) AS DATE))  AS delivery_date
      FROM bronze.zmm30 WHERE NULLIF(TRIM(purchase_order), '') IS NOT NULL GROUP BY 1, 2
    ),
    lastpo AS (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY material, mpn ORDER BY document_date DESC, po DESC, po_item DESC) AS rn FROM po
    )
    SELECT g.material, g.mpn, g.po_count, g.last_po_date,
           l.po AS last_po_number, l.po_qty AS last_po_qty, l.uom AS last_po_unit,
           l.rate AS last_po_rate, l.currency AS last_po_currency,
           z.created_by AS last_po_created_by, z.created_on AS last_po_created_on, z.delivery_date AS last_po_delivery_date
    FROM (SELECT material, mpn, COUNT(DISTINCT po) AS po_count, MAX(document_date) AS last_po_date FROM po GROUP BY 1, 2) g
    JOIN lastpo l ON l.material = g.material AND l.mpn = g.mpn AND l.rn = 1
    LEFT JOIN z ON z.po = l.po AND z.po_item = l.po_item""")
    return spark.sql("""
    WITH avl AS (
      SELECT DISTINCT
        CAST(TRIM(item_code) AS STRING)                                                  AS item_code,
        material_description,
        CAST(TRIM(manufacturer_part_no) AS STRING)                                       AS mpn,
        CAST(TRIM(manufacturer_code) AS STRING)                                          AS manufacturer_code,
        manufacturer_name,
        NULLIF(TRIM(manufacturer_country), '')                                           AS manufacturer_country,
        NULLIF(TRIM(storage_condition), '')                                              AS storage_condition,
        CAST(TRY_CAST(TRY_CAST(NULLIF(TRIM(supplier_code), '') AS DOUBLE) AS BIGINT) AS STRING) AS supplier_code,
        supplier_name,
        TRY_CAST(LEFT(TRIM(qualification_due_on), 10) AS DATE)                           AS qual_due,
        COALESCE(TRY_CAST(LEFT(TRIM(qualification_due_on), 10) AS DATE) > DATE'2050-01-01', FALSE) AS date_suspect
      FROM bronze.avl
    )
    SELECT sha2(concat_ws('|', a.item_code, a.material_description, a.mpn, a.manufacturer_code, a.manufacturer_name,
                          a.supplier_code, a.supplier_name, CAST(a.qual_due AS STRING)), 256) AS avl_key,
           a.*, h.po_count, h.last_po_date, h.last_po_number, h.last_po_qty, h.last_po_unit, h.last_po_rate, h.last_po_currency,
           h.last_po_created_by, h.last_po_created_on, h.last_po_delivery_date
    FROM avl a LEFT JOIN po_by_mpn h ON h.material = a.item_code AND h.mpn = a.mpn""")

merge_snapshot(build_avl, "dim_avl_vendor", ["avl_key"], sources=["bronze.avl", "bronze.me2n", "bronze.zmm30"])

# dim_po_rate : one row per PO item (history of PO rates per material + MPN), same scope as above. Gold computes the 12-month
# average / min / max from it every day. rate = PO value / PO quantity, in the PO's own currency and unit (no conversion).
def build_po_rate():
    make_po_clean()
    return spark.sql("SELECT material, mpn, po, po_item, document_date, po_qty, uom, po_value, rate, currency FROM po_clean")

merge_snapshot(build_po_rate, "dim_po_rate", ["po", "po_item"], sources=["bronze.me2n"])

# COMMAND ----------

# dim_vendor_price : last agreed price and delivery terms per item (parent material) x MPN x supplier x plant, from the Info Record list.
# Rule (client + approved): deleted info records ignored; a price of 0 means "not maintained" (net_price stays NULL);
# among the records of one item/MPN/supplier/plant the one WITH a price and the latest Valid From wins (info record number breaks ties).
# Gold picks the PR plant's record first, then the latest overall. Dates arrive either as dates or as Excel serial numbers: both handled.
def _xdate(c):
    return (f"CASE WHEN TRIM({c}) RLIKE '^[0-9]+(\\\\.0+)?$' THEN date_add(DATE'1899-12-30', CAST(CAST(TRIM({c}) AS DOUBLE) AS INT)) "
            f"ELSE TRY_CAST(LEFT(TRIM({c}), 10) AS DATE) END")

def build_vendor_price():
    return spark.sql(f"""
    WITH ir AS (
      SELECT TRIM(parent_material) AS item_code, TRIM(material) AS mpn, TRIM(supplier) AS supplier_code, TRIM(plant) AS plant,
             TRIM(purchasing_info_rec) AS info_record,
             TRY_CAST(net_price AS DOUBLE) AS net_price_raw, UPPER(TRIM(currency)) AS currency,
             TRY_CAST(price_unit AS DOUBLE) AS price_unit, TRIM(order_price_unit) AS order_price_unit,
             {_xdate('valid_from')} AS valid_from, {_xdate('valid_to')} AS valid_to,
             TRY_CAST(planned_deliv_time AS INT) AS lead_time_days,
             TRY_CAST(standard_po_quantity AS DOUBLE) AS standard_po_qty,
             TRY_CAST(overdeliv_tolerance AS DOUBLE) AS over_delivery_tolerance_pct,
             UPPER(TRIM(source_list_y_n)) AS source_list, NULLIF(TRIM(purchasing_group), '') AS purchasing_group
      FROM bronze.info_record
      WHERE NULLIF(TRIM(parent_material), '') IS NOT NULL AND NULLIF(TRIM(material), '') IS NOT NULL
        AND NULLIF(TRIM(deletion_indicator), '') IS NULL
    ),
    r AS (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY item_code, mpn, supplier_code, plant
                                   ORDER BY (net_price_raw > 0) DESC NULLS LAST, valid_from DESC NULLS LAST, info_record DESC) AS rn
      FROM ir
    )
    SELECT item_code, mpn, supplier_code, plant, info_record,
           CASE WHEN net_price_raw > 0 THEN net_price_raw END AS net_price,
           currency, price_unit, order_price_unit,
           CASE WHEN net_price_raw > 0 AND price_unit > 0 THEN net_price_raw / price_unit END AS price_per_unit,
           valid_from, valid_to, lead_time_days, standard_po_qty, over_delivery_tolerance_pct, source_list, purchasing_group
    FROM r WHERE rn = 1""")

merge_snapshot(build_vendor_price, "dim_vendor_price", ["item_code", "mpn", "supplier_code", "plant"], sources=["bronze.info_record"])

# COMMAND ----------

NUMU = r"\d+(?:\.\d+)?"
UNITS = r'(?:g/m²|g/m2|gsm|µg/g|ug/g|mg/l|mg/ml|µm|µ|um|microns?|inches|inch|mm|cm|nm|kg|mg|ml|cc|g|%|°c|")'
RULES = [
 # (priority, rule_name, kind, regex, lo_group, hi_group)   kind: RANGE | MAX | MIN | EXACT | PLUSMINUS (lo = centre, hi = tolerance)
 (10, "between_a_and_b",   "RANGE",     rf"(?i)^\W*between\s+({NUMU})\s*{UNITS}?\s*(?:and|to|-)\s*({NUMU})", 1, 2),
 (20, "nlt_a_nmt_b",       "RANGE",     rf"(?i)(?:not less than|nlt|>=|at least|minimum)\s*({NUMU})\s*{UNITS}?\W{{0,12}}(?:and\s*)?(?:not more than|nmt|<=|maximum)\s*({NUMU})", 1, 2),
 (30, "centre_plus_minus", "PLUSMINUS", rf"({NUMU})\s*{UNITS}?\s*(?:±|\+/-|\+-)\s*({NUMU})", 1, 2),
 (40, "max_words",          "MAX",       rf"(?i)(?:not more than|nmt|<=|does not exceed|not exceed|maximum|max\.?|up to|less than or equal to|less than|below)[^0-9]{{0,20}}?({NUMU})", 0, 1),
 (35, "not_differ_by_more_than", "MAX", rf"(?i)not differ\b.{{0,200}}?by\s+(?:more|greater) than\s*({NUMU})", 0, 1),   # "does not differ from the reference by more than 6 C" is a MAXIMUM difference
 (50, "min_words",          "MIN",       rf"(?i)(?:not less than|nlt|>=|at least|minimum|min\.?|more than|greater than|above)\s*({NUMU})", 1, 0),
 (60, "range_loose",        "RANGE",     rf"(?i)(?<![\w.])({NUMU})\s*{UNITS}?\s*(?:to|-)\s*({NUMU})(?![\w.])", 1, 2),
 (70, "exact_value_unit",   "EXACT",     rf"(?i)^\W*({NUMU})\s*{UNITS}\s*$", 1, 1),
 (5,  "narrative_solution", "TEXT",      r"(?i)^\W*(?:the\s+)?solution\b", 0, 0),      # kind TEXT = wording only, never numbers ("Solution C1 is clear if...")
]
RULES_TBL = "silver_spec_parse_rules"
spark.sql(f"""CREATE TABLE IF NOT EXISTS {RULES_TBL} (
  priority INT, rule_name STRING, kind STRING, regex STRING, lo_group INT, hi_group INT, enabled BOOLEAN, is_builtin BOOLEAN)""")
if "is_builtin" not in spark.table(RULES_TBL).columns:                         # table made by an earlier version of this notebook
    spark.sql(f"ALTER TABLE {RULES_TBL} ADD COLUMNS (is_builtin BOOLEAN)")
# Built-in rules (the list above) are refreshed on every run and keep the 'enabled' switch you set.
# Rules you add yourself (is_builtin = false) are never touched.
old_enabled = {r.rule_name: r.enabled for r in spark.table(RULES_TBL).select("rule_name", "enabled").collect()}
spark.sql(f"DELETE FROM {RULES_TBL} WHERE rule_name IN ({', '.join(repr(r[1]) for r in RULES)})")
spark.createDataFrame([r + (old_enabled.get(r[1], True), True) for r in RULES],
    "priority int, rule_name string, kind string, regex string, lo_group int, hi_group int, enabled boolean, is_builtin boolean") \
    .write.mode("append").saveAsTable(RULES_TBL)
display(spark.table(RULES_TBL).orderBy("priority"))
# To support a new wording, add a row (is_builtin = false), for example:
#   INSERT INTO silver_spec_parse_rules VALUES (45, 'ceiling_of', 'MAX', '(?i)ceiling of\\s*(\\d+(?:\\.\\d+)?)', 0, 1, true, false);
# The next run of the parse cell notices that the rules changed and re-parses every spec by itself (no flag needed).

# COMMAND ----------

_t_parse = datetime.now()
FORCE_REPARSE = False      # True = re-parse ALL specs. Normally not needed: a change in the rule table is detected automatically.
PARSED_TBL = "dim_spec_test_parsed"

_rule_rows = spark.table(RULES_TBL).where("enabled").orderBy("rule_name").select(
    F.concat_ws("~", "rule_name", "kind", "regex", F.col("priority").cast("string"),
                F.col("lo_group").cast("string"), F.col("hi_group").cast("string"))).collect()
RULES_HASH = spark.range(1).select(F.sha2(F.lit("\n".join(r[0] for r in _rule_rows)), 256)).first()[0]

def parse_tests(tests):
    t = (tests
        .where(F.lower(F.trim(F.col("is_group_header"))) != "true")                        # section headings carry no limit
        .select(
            F.regexp_extract(F.element_at(F.split("file_path", "/"), -1), r"^(\d+)", 1).alias("mpn"),
            F.element_at(F.split("file_path", "/"), -1).alias("file_name"),
            "specification_no", "edition_no",
            F.col("test_seq").cast("int").alias("test_seq"), "sr_no",
            F.when(F.lower(F.trim("test")).isin("null", "none", ""), None).otherwise(F.trim("test")).alias("test_raw"),
            F.when(F.lower(F.trim("specification")).isin("null", "none", ""), None).otherwise(F.trim("specification")).alias("spec_raw"),
            "method_reference", "h_extracted_at", "h_file_modified_at", F.lit(RULES_HASH).alias("rules_hash")))

    # --- test name -> parameter + component hint. "Total Height > Outer Cap" => parameter "total height", hint outer
    tn = F.lower(F.regexp_replace(F.coalesce("test_raw", F.lit("")), "[“”\"'`Ø]", ""))
    t = (t.withColumn("tn", F.trim(F.regexp_replace(tn, r"\s+", " ")))
          .withColumn("tn_param", F.trim(F.split("tn", ">").getItem(0)))
          .withColumn("tn_hint",  F.trim(F.expr("get(split(tn, '>'), 1)")))   # get() returns NULL when there is no '>'
          .withColumn("comp_only", F.col("tn").rlike(r"^(outer|inner)( cap)?$"))
          .withColumn("name_comp",
              F.when(F.col("tn_hint").rlike("outer"), "outer").when(F.col("tn_hint").rlike("inner"), "inner")
               .when(F.col("tn").rlike(r"^outer( cap)?$"), "outer").when(F.col("tn").rlike(r"^inner( cap)?$"), "inner")))
    # a row that is only "Outer Cap" / "Inner Cap" belongs to the previous real test of the same spec
    w = Window.partitionBy("specification_no", "edition_no").orderBy("test_seq").rowsBetween(Window.unboundedPreceding, -1)
    t = (t.withColumn("prev_param", F.last(F.when(F.col("comp_only"), None).otherwise(F.col("tn_param")), ignorenulls=True).over(w))
          .withColumn("parameter", F.when(F.col("comp_only"), F.col("prev_param")).otherwise(F.col("tn_param"))))

    # --- clean the limit text, split into lines
    c = F.col("spec_raw")
    c = F.regexp_replace(c, "[   ]", " ")
    c = F.regexp_replace(c, "[–—−]", "-")
    c = F.regexp_replace(c, "≥", ">=")
    c = F.regexp_replace(c, "≤", "<=")
    c = F.regexp_replace(c, "μ", "µ")
    c = F.regexp_replace(c, r"(?<=\d),(?=\d{3}(?!\d))", "")                         # 1,000 -> 1000
    t = t.withColumn("spec_clean", c)
    lines = F.filter(F.split("spec_clean", r"\n"), lambda x: F.length(F.trim(x)) > 0)
    t = (t.withColumn("n_seg", F.coalesce(F.size(lines), F.lit(0)))
          .withColumn("seg_pair", F.explode_outer(F.arrays_zip(F.sequence(F.lit(1), F.greatest(F.size(lines), F.lit(1))).alias("n"), lines.alias("line"))))
          .withColumn("seg_no", F.coalesce(F.col("seg_pair.n"), F.lit(1)))
          .withColumn("seg", F.trim(F.col("seg_pair.line"))).drop("seg_pair"))

    # --- component of this line: a label at the start of the line wins, then the test-name hint
    t = (t.withColumn("seg_comp",
              F.when(F.col("seg").rlike(r"(?i)^[^a-z]*outer"), "outer").when(F.col("seg").rlike(r"(?i)^[^a-z]*inner"), "inner")
               .when(F.col("seg").rlike(r"(?i)^[^a-z]*combined"), "combined"))
          .withColumn("component", F.coalesce("seg_comp", "name_comp"))
          .withColumn("body", F.when(F.col("seg_comp").isNotNull() & F.col("seg").contains(":"),
                                     F.regexp_replace("seg", r"^[^:]*:\s*", "")).otherwise(F.col("seg")))
          .withColumn("key", F.concat_ws("|", "specification_no", "edition_no", F.col("test_seq").cast("string"), F.col("seg_no").cast("string"))))

    # --- apply the rules: the first matching rule (lowest priority number) wins
    rules = F.broadcast(spark.table(RULES_TBL).where("enabled").select(
        F.col("priority").alias("r_pri"), F.col("rule_name").alias("r_name"), F.col("kind").alias("r_kind"),
        F.col("regex").alias("r_rx"), F.col("lo_group").alias("r_lo"), F.col("hi_group").alias("r_hi")))
    hit = (t.select("key", "body").where(F.col("body").isNotNull()).crossJoin(rules)
            .where(F.expr("body rlike r_rx"))
            .withColumn("rn", F.row_number().over(Window.partitionBy("key").orderBy("r_pri")))
            .where("rn = 1")
            .select("key", "r_name", "r_kind",
                    F.expr("CASE WHEN r_lo > 0 THEN TRY_CAST(NULLIF(regexp_extract(body, r_rx, r_lo), '') AS DOUBLE) END").alias("v1"),
                    F.expr("CASE WHEN r_hi > 0 THEN TRY_CAST(NULLIF(regexp_extract(body, r_rx, r_hi), '') AS DOUBLE) END").alias("v2")))
    t = t.join(hit, "key", "left")
    t = (t.withColumn("raw_min", F.when(F.col("r_kind") == "RANGE", F.least("v1", "v2")).when(F.col("r_kind").isin("MIN", "EXACT"), F.col("v1"))
                                  .when(F.col("r_kind") == "PLUSMINUS", F.col("v1") - F.col("v2")))
          .withColumn("raw_max", F.when(F.col("r_kind") == "RANGE", F.greatest("v1", "v2")).when(F.col("r_kind") == "MAX", F.col("v2"))
                                  .when(F.col("r_kind") == "EXACT", F.col("v1")).when(F.col("r_kind") == "PLUSMINUS", F.col("v1") + F.col("v2"))))

    # --- unit: the first number that is directly followed by a unit
    unit_rx = r'(?i)\d\s*(g/m²|g/m2|gsm|µg/g|ug/g|mg/l|mg/ml|µm|µ|um|microns?|inches|inch|mm|cm|nm|kg|mg|ml|cc|g|%|°c|")(?![a-zA-Z²/])'
    t = t.withColumn("unit_raw", F.lower(F.nullif(F.regexp_extract("body", unit_rx, 1), F.lit(""))))
    t = (t.withColumn("unit", F.when(F.col("unit_raw").isin("inch", "inches", '"'), "mm")
                               .when(F.col("unit_raw").isin("micron", "microns", "um", "µ"), "µm")
                               .when(F.col("unit_raw") == "gsm", "g/m²").when(F.col("unit_raw") == "g/m2", "g/m²")
                               .when(F.col("unit_raw") == "ug/g", "µg/g").otherwise(F.col("unit_raw")))
          .withColumn("factor", F.when(F.col("unit_raw").isin("inch", "inches", '"'), F.lit(25.4)).otherwise(F.lit(1.0)))
          .withColumn("spec_min", F.round(F.col("raw_min") * F.col("factor"), 4))
          .withColumn("spec_max", F.round(F.col("raw_max") * F.col("factor"), 4)))

    # --- status: nothing is dropped, every line says how it was read
    unlabelled_multi = (F.col("n_seg") > 1) & F.col("seg_comp").isNull()           # several values and no way to tell which part each belongs to
    t = t.withColumn("parse_status",
          F.when(F.col("seg").isNull(), "NO_SPEC_TEXT")
           .when(F.col("comp_only") & (F.col("n_seg") > 1), "SKIPPED_SUMMARY_ROW")      # an "Outer Cap" row holding two values repeats the labelled rows above it
           .when(F.col("r_kind") == "TEXT", "TEXT_ONLY")                                  # a rule says: wording only
           .when(F.col("r_name").isNotNull() & unlabelled_multi, "MULTI_VALUE_UNLABELLED")
           .when(F.col("r_name").isNotNull(), F.concat(F.lit("PARSED_"), F.col("r_kind")))
           .when(~F.col("body").rlike(r"\d"), "TEXT_ONLY")
           .when(F.col("tn").rlike(r"^(description|appearance|identification)\b"), "TEXT_ONLY")   # narrative tests that mention a size, e.g. "625 cc bottle"
           .otherwise("NUMBER_NOT_PARSED"))
    numeric = F.col("parse_status").startswith("PARSED_")
    return t.select(
        "key", "mpn", "file_name", "specification_no", "edition_no", "test_seq", "seg_no", "sr_no",
        "test_raw", "parameter", "component", F.col("spec_raw").alias("spec_text"), F.col("seg").alias("spec_line"),
        F.when(numeric, F.col("spec_min")).alias("spec_min"), F.when(numeric, F.col("spec_max")).alias("spec_max"),
        F.when(numeric, F.col("unit")).alias("unit"),
        F.when(numeric, F.col("raw_min")).alias("spec_min_raw"), F.when(numeric, F.col("raw_max")).alias("spec_max_raw"),
        F.col("unit_raw").alias("unit_raw"), "parse_status", F.col("r_name").alias("parse_rule"),
        "method_reference", "h_extracted_at", "h_file_modified_at", "rules_hash")

bt = spark.table(f"{SOURCE_CATALOG}.samwed.spec_tests")
hd = spark.table(f"{SOURCE_CATALOG}.samwed.spec_header").select(
        "specification_no", "edition_no",
        F.col("extracted_at").alias("h_extracted_at"), F.col("file_modified_at").alias("h_file_modified_at")).distinct()
exists = spark.catalog.tableExists(PARSED_TBL)
rules_changed = exists and ("rules_hash" not in spark.table(PARSED_TBL).columns or
    spark.table(PARSED_TBL).where(F.col("rules_hash") != F.lit(RULES_HASH)).limit(1).count() > 0)
if rules_changed:
    print("parse rules changed since the last run: re-parsing every spec")
reparse_all = FORCE_REPARSE or not exists or rules_changed

# A spec is new or changed when its header (spec, edition, extracted_at, file_modified_at) is not in the parsed table yet.
if reparse_all:
    affected = hd
else:
    done = spark.table(PARSED_TBL).select("specification_no", "edition_no", "h_extracted_at", "h_file_modified_at").distinct()
    cond = [hd[c].eqNullSafe(done[c]) for c in ["specification_no", "edition_no", "h_extracted_at", "h_file_modified_at"]]
    affected = hd.join(done, cond, "left_anti")
n_aff = affected.count()
print(f"specs to (re)parse: {n_aff}")

if n_aff > 0:
    # all test rows of those specs: an 'Outer Cap' row needs the row before it
    spec_rows = bt.join(affected, ["specification_no", "edition_no"])
    parsed = parse_tests(spec_rows)
    if reparse_all:
        parsed.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(PARSED_TBL)
    else:
        affected.select("specification_no", "edition_no").createOrReplaceTempView("spec_parse_keys")
        spark.sql(f"""DELETE FROM {PARSED_TBL} WHERE CONCAT_WS('|', specification_no, edition_no) IN
                      (SELECT CONCAT_WS('|', specification_no, edition_no) FROM spec_parse_keys)""")
        parsed.write.mode("append").saveAsTable(PARSED_TBL)
display(spark.table(PARSED_TBL).groupBy("parse_status").count().orderBy("parse_status"))
meta_upsert(PARSED_TBL, "incremental", ["key"], _t_parse, "Succeeded")

# COMMAND ----------

# One row per specification document. Rebuilt on every run (it is small) from bronze_spec_headers, bronze_spec_approval
# and the parsed test lines, so a new edition automatically becomes the latest one for its MPN.
# is_latest = newest effective date, then highest edition, per MPN (MPN = digits at the start of the file name).
# 'null' / 'None' text from the extraction becomes a real NULL. approvals = signatories as an array (stage, name, date...).
def clean(col):
    return f"CASE WHEN LOWER(TRIM({col})) IN ('null','none','') THEN NULL ELSE TRIM({col}) END"

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW approvals_agg AS
SELECT specification_no, edition_no,
       sort_array(collect_list(named_struct(
         'signatory_seq', TRY_CAST(signatory_seq AS INT), 'stage', stage,
         'department', {clean('department')}, 'name', {clean('name')}, 'designation', {clean('designation')},
         'sign_date', TRY_CAST(sign_date AS DATE)))) AS approvals
FROM {SOURCE_CATALOG}.samwed.spec_approvals GROUP BY specification_no, edition_no
""")

df_spec_doc = spark.sql(f"""
WITH h AS (
  SELECT regexp_extract(file_name, '^([0-9]+)', 1) AS mpn, file_name, specification_no, edition_no,
         {clean('material_name')}                AS material_name,
         UPPER({clean('material_type')})         AS material_type,
         {clean('supersedes_no')}                AS supersedes_no,
         {clean('manufacturer')}                 AS pdf_manufacturer,
         {clean('manufacturer_code')}            AS pdf_manufacturer_code,
         {clean('material_code')}                AS pdf_material_code,
         TRY_CAST(effective_date AS DATE)        AS effective_date, extracted_at
  FROM {SOURCE_CATALOG}.samwed.spec_header
),
n AS (
  SELECT specification_no, edition_no, COUNT(*) AS n_lines,
         SUM(CASE WHEN parse_status LIKE 'PARSED_%' THEN 1 ELSE 0 END) AS n_numeric_limits,
         SUM(CASE WHEN parse_status IN ('NUMBER_NOT_PARSED','MULTI_VALUE_UNLABELLED') THEN 1 ELSE 0 END) AS n_needs_review
  FROM dim_spec_test_parsed GROUP BY specification_no, edition_no
)
SELECT h.*, COALESCE(n.n_lines, 0) AS n_lines, COALESCE(n.n_numeric_limits, 0) AS n_numeric_limits,
       COALESCE(n.n_needs_review, 0) AS n_needs_review,
       ROW_NUMBER() OVER (PARTITION BY h.mpn ORDER BY h.effective_date DESC NULLS LAST,
                          TRY_CAST(h.edition_no AS INT) DESC NULLS LAST, h.extracted_at DESC) = 1 AS is_latest,
       a.approvals
FROM h
LEFT JOIN n ON n.specification_no = h.specification_no AND n.edition_no = h.edition_no
LEFT JOIN approvals_agg a ON a.specification_no = h.specification_no AND a.edition_no = h.edition_no
""")
merge_snapshot(lambda: df_spec_doc, "dim_spec_doc", ["file_name", "specification_no", "edition_no"])   # always runs (small); writes only changed rows
display(spark.table("dim_spec_doc").select("mpn", "specification_no", "edition_no", "effective_date", "is_latest", "n_lines", "n_numeric_limits", "n_needs_review").orderBy("mpn"))

# COMMAND ----------

# Checks. Expected on the current extract (846 ME5A lines, 263 PRs): 170 PM lines in 23 PRs, OPEN 150 / PARTIALLY_ORDERED 20,
# 35 lines with L1 release missing, 8 buyers, every line on a FINAL_RELEASE clock.
display(spark.sql("SELECT category, subcategory, COUNT(*) AS lines, COUNT(DISTINCT pr) AS prs FROM dim_pr_open_pkg GROUP BY 1, 2"))
display(spark.sql("SELECT pr_status, COUNT(*) AS lines FROM dim_pr_open_pkg GROUP BY 1 ORDER BY 1"))
display(spark.sql("""SELECT SUM(CASE WHEN l1_release_missing THEN 1 ELSE 0 END) AS l1_missing,
                            SUM(CASE WHEN buyer IS NULL THEN 1 ELSE 0 END) AS no_buyer,
                            COUNT(DISTINCT buyer) AS buyers FROM dim_pr_open_pkg"""))
display(spark.sql("SELECT tat_start_source, COUNT(*) AS lines FROM dim_pr_open_pkg GROUP BY 1"))

# TAT preview computed on the fly (gold will hold this as a view). Business days = Mon-Fri, start date up to but not including today.
# Expected on 2026-11-12: Missed 6-10 days 4, Missed >10 days 36, Risk 130, Within 0 (drifts as days pass).
display(spark.sql("""
WITH t AS (
  SELECT *, CASE WHEN tat_start_date >= current_date() THEN 0
                 ELSE size(filter(sequence(tat_start_date, date_sub(current_date(), 1), INTERVAL 1 DAY), d -> dayofweek(d) NOT IN (1, 7))) END AS elapsed
  FROM dim_pr_open_pkg)
SELECT CASE WHEN elapsed > tat_standard_days THEN 'Missed'
            WHEN tat_standard_days - elapsed <= tat_risk_max_remaining THEN 'Risk' ELSE 'Within' END AS tat_status,
       CASE WHEN elapsed <= tat_standard_days THEN NULL
            WHEN elapsed <= tat_missed_bucket1_max_elapsed THEN CONCAT(tat_standard_days + 1, '-', tat_missed_bucket1_max_elapsed, ' days')
            ELSE CONCAT('>', tat_missed_bucket1_max_elapsed, ' days') END AS missed_bucket,
       COUNT(*) AS lines
FROM t GROUP BY 1, 2 ORDER BY 1, 2"""))

# Info Record list: expect every purchased (item, MPN) pair to have a record; prices only where > 0.
display(spark.sql("""SELECT COUNT(*) AS price_rows, SUM(CASE WHEN net_price IS NOT NULL THEN 1 ELSE 0 END) AS with_price,
                            SUM(CASE WHEN valid_to >= current_date() THEN 1 ELSE 0 END) AS valid_today_or_later FROM silver.dim_vendor_price"""))
display(spark.sql("""SELECT COUNT(*) AS purchased_pairs, SUM(CASE WHEN p.mpn IS NOT NULL THEN 1 ELSE 0 END) AS with_info_record
                     FROM (SELECT DISTINCT item_code, mpn FROM silver.dim_avl_vendor WHERE po_count IS NOT NULL) a
                     LEFT JOIN (SELECT DISTINCT item_code, mpn FROM silver.dim_vendor_price) p ON p.item_code = a.item_code AND p.mpn = a.mpn"""))
display(spark.sql("SELECT COUNT(*) AS purchased_pairs_with_last_po_qty FROM silver.dim_avl_vendor WHERE last_po_qty IS NOT NULL"))

# What the last run did to each silver table (common metadata table):
display(spark.sql(f"SELECT * FROM {METADATA_TBL} WHERE schema_name = '{SCHEMA}' ORDER BY table_id"))
