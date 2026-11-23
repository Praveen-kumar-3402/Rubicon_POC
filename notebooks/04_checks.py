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
    LEFT JOIN (SELECT pr, MAX(action_status = 'Has_Exceptions') AS h FROM {CATALOG}.gold.v_pr_summary GROUP BY pr) s ON s.pr = l.pr
    WHERE l.has_exception_line <> COALESCE(s.h, false)""") == 0)

check("has_spec lines carry a specification and a match type", q(f"""SELECT COUNT(*) FROM {CATALOG}.gold.v_vendor_options
    WHERE has_spec AND (specification_no IS NULL OR spec_match NOT IN ('MPN','MATERIAL_CODE'))""") == 0)
check("a vendor without a spec has no match type", q(f"""SELECT COUNT(*) FROM {CATALOG}.gold.v_vendor_options
    WHERE NOT has_spec AND spec_match IS NOT NULL""") == 0)

check("PO rate history is not empty and has no zero or negative rates", q(f"SELECT COUNT(*) FROM {CATALOG}.silver.dim_po_rate WHERE rate IS NULL OR rate <= 0") == 0
      and q(f"SELECT COUNT(*) FROM {CATALOG}.silver.dim_po_rate") > 0)
check("every purchased vendor has a last PO rate and currency", q(f"""SELECT COUNT(*) FROM {CATALOG}.silver.dim_avl_vendor
    WHERE po_count IS NOT NULL AND (last_po_rate IS NULL OR last_po_currency IS NULL)""") == 0)
check("12-month min <= average <= max", q(f"""SELECT COUNT(*) FROM {CATALOG}.gold.v_vendor_options
    WHERE rate_12m_avg IS NOT NULL AND NOT (rate_12m_min <= rate_12m_avg + 1e-9 AND rate_12m_avg <= rate_12m_max + 1e-9)""") == 0)
check("PO vs Info Record flag only when a comparison exists", q(f"""SELECT COUNT(*) FROM {CATALOG}.gold.v_vendor_options
    WHERE (po_vs_info_record_pct IS NULL) <> (po_vs_info_record_flag IS NULL)""") == 0)
check("cancelled-and-reopened PR lines list their POs", q(f"""SELECT COUNT(*) FROM {CATALOG}.gold.v_pr_summary
    WHERE po_cancelled_reopened AND cancelled_pos IS NULL""") == 0)

# 4. the pipeline recorded itself and nothing failed
check("no Failed rows in pipeline_metadata", q(f"SELECT COUNT(*) FROM {CATALOG}.metadata.pipeline_metadata WHERE last_run_status = 'Failed'") == 0)

assert not failures, f"{len(failures)} check(s) failed: {failures}"
print("All checks passed on", CATALOG)
