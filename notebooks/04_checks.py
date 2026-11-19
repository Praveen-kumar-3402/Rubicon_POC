# Databricks notebook source
# MAGIC %md
# MAGIC ##### 04_checks - automated tests after a run. Any failed assertion fails the job, which stops the promotion to the next environment.

# COMMAND ----------

dbutils.widgets.text("catalog", "rubicon_dev", "Target catalog")
CATALOG = dbutils.widgets.get("catalog")
q = lambda sql: spark.sql(sql).first()[0]
failures = []

def check(name, ok, detail=""):
    print(("PASS  " if ok else "FAIL  ") + name, detail)
    if not ok: failures.append(name)

# 1. every layer has data
for t in ["bronze.me5a", "bronze.me2n", "bronze.zmm30", "bronze.avl", "bronze.info_record",
          "silver.dim_pr_open_pkg", "silver.dim_avl_vendor", "silver.dim_vendor_price", "silver.dim_spec_doc"]:
    check(f"{t} is not empty", q(f"SELECT COUNT(*) FROM {CATALOG}.{t}") > 0)

# 2. scope: packaging only, one row per PR line
check("silver PR lines are PM only", q(f"SELECT COUNT(*) FROM {CATALOG}.silver.dim_pr_open_pkg WHERE subcategory <> 'PM'") == 0)
check("pr_line is unique", q(f"SELECT COUNT(*) - COUNT(DISTINCT pr_line) FROM {CATALOG}.silver.dim_pr_open_pkg") == 0)

# 3. gold views answer, and action_status follows the exception count
for v in ["v_vendor_options", "v_vendor_spec", "v_pr_tat", "v_pr_exceptions", "v_pr_summary", "v_pr_list", "v_buyer_summary"]:
    check(f"gold.{v} is readable", spark.sql(f"SELECT * FROM {CATALOG}.gold.{v} LIMIT 1").columns is not None)
check("action_status = exception count rule", q(f"""SELECT COUNT(*) FROM {CATALOG}.gold.v_pr_summary
    WHERE (action_status = 'No_Exception') <> (exception_count = 0)""") == 0)
check("every PR line has an action_status", q(f"SELECT COUNT(*) FROM {CATALOG}.gold.v_pr_summary WHERE action_status IS NULL") == 0)
check("TAT status is Missed / Risk / Within", q(f"""SELECT COUNT(*) FROM {CATALOG}.gold.v_pr_summary
    WHERE tat_status NOT IN ('Missed','Risk','Within')""") == 0)

check("every PR is under at least one exception status", q(f"""SELECT COUNT(*) FROM {CATALOG}.gold.v_pr_list
    WHERE NOT (has_no_exception_line OR has_exception_line)""") == 0)
check("PR flags match its lines", q(f"""SELECT COUNT(*) FROM {CATALOG}.gold.v_pr_list l
    WHERE has_exception_line <> EXISTS (SELECT 1 FROM {CATALOG}.gold.v_pr_summary s WHERE s.pr = l.pr AND s.action_status = 'Has_Exceptions')""") == 0)

# 4. the pipeline recorded itself and nothing failed
check("no Failed rows in pipeline_metadata", q(f"SELECT COUNT(*) FROM {CATALOG}.metadata.pipeline_metadata WHERE last_run_status = 'Failed'") == 0)

assert not failures, f"{len(failures)} check(s) failed: {failures}"
print("All checks passed on", CATALOG)
