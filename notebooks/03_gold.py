# Databricks notebook source
# MAGIC %md
# MAGIC ##### Notebook Name : Gold_Vendor_Discovery_NB
# MAGIC - **Created at** (dd/mm/yyyy) : 06/11/2026
# MAGIC - **Modified at** (dd/mm/yyyy) : 12/11/2026
# MAGIC - **Description** : Gold views for the PR storyline dashboard. Built on the existing views (kept as they were, including your wording) with these additions: TAT per PR line (`v_pr_tat`), buyer, category, TAT and action / non action on `v_pr_summary` and `v_pr_list`, the `L1_RELEASE_MISSING` exception, and `v_buyer_summary`.
# MAGIC
# MAGIC **Run order:** v_vendor_options -> v_vendor_spec -> v_pr_tat -> v_pr_exceptions -> v_pr_summary -> v_pr_list -> v_buyer_summary.
# MAGIC **Action / non action (per PR line):** ACTION = at least one approved vendor with a specification on file. NON_ACTION = the path breaks, with `non_action_reason`. A PR is ACTION only when every line is ACTION.

# COMMAND ----------

from pyspark.sql import functions as F, Window
import re

# COMMAND ----------

dbutils.widgets.text("catalog", "rubicon_dev", "Target catalog (rubicon_dev / rubicon_qa / rubicon_prod)")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA  = "gold"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}") 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Added: last agreed price, lead time, source list, manufacturer country, last PO details (price terms only for vendors we have purchased from).
# MAGIC -- One row per PR line x approved vendor (x supplier row). Lines without any AVL entry appear once with mpn = NULL.
# MAGIC -- qual_status is computed here (not stored) because it changes every day. 90 days for EXPIRING is a placeholder to confirm with the client.
# MAGIC -- vendor_role: CURRENT = bought in last 12 months, HISTORICAL = bought earlier, ALTERNATIVE_UNUSED = approved, never bought.
# MAGIC -- last_used_rank = 1 marks the most recently used vendor (the baseline for spec comparison).
# MAGIC -- source_type has four explicit values. Spec header columns come from the vendor's latest specification.
# MAGIC CREATE OR REPLACE VIEW v_vendor_options AS
# MAGIC WITH vp AS (   -- last agreed price per PR line x MPN x supplier: a record WITH a price first, then the PR plant, then the latest Valid From
# MAGIC   SELECT * FROM (
# MAGIC     SELECT p.pr_line, v.mpn, v.supplier_code, v.net_price, v.currency, v.price_unit, v.order_price_unit, v.price_per_unit,
# MAGIC            v.valid_from, v.valid_to, v.lead_time_days, v.standard_po_qty, v.over_delivery_tolerance_pct, v.source_list,
# MAGIC            ROW_NUMBER() OVER (PARTITION BY p.pr_line, v.mpn, v.supplier_code
# MAGIC                               ORDER BY (v.net_price IS NOT NULL) DESC, (v.plant = p.plant) DESC, v.valid_from DESC NULLS LAST) AS rn
# MAGIC     FROM silver.dim_pr_open_pkg p JOIN silver.dim_vendor_price v ON v.item_code = p.material) WHERE rn = 1
# MAGIC ),
# MAGIC opts AS (
# MAGIC   SELECT p.pr_line, p.pr, p.pr_item, p.plant, p.pr_date, p.qty, p.uom,
# MAGIC          p.material, p.short_text, p.pack_category,
# MAGIC          a.mpn, a.manufacturer_name, a.supplier_code, a.supplier_name, a.qual_due, a.po_count, a.last_po_date,
# MAGIC          a.manufacturer_country, a.storage_condition, a.last_po_number, a.last_po_qty, a.last_po_unit,
# MAGIC          a.last_po_created_by, a.last_po_created_on, a.last_po_delivery_date,
# MAGIC          CASE WHEN a.po_count IS NOT NULL THEN CASE WHEN vp.net_price IS NULL THEN 'No agreed price on file' ELSE 'Price on file' END END AS price_status,
# MAGIC          CASE WHEN a.po_count IS NOT NULL THEN vp.net_price END           AS price_net,
# MAGIC          CASE WHEN a.po_count IS NOT NULL THEN vp.currency END            AS price_currency,
# MAGIC          CASE WHEN a.po_count IS NOT NULL THEN vp.price_unit END          AS price_unit_qty,
# MAGIC          CASE WHEN a.po_count IS NOT NULL THEN vp.order_price_unit END    AS price_order_unit,
# MAGIC          CASE WHEN a.po_count IS NOT NULL THEN vp.price_per_unit END      AS price_per_unit,
# MAGIC          CASE WHEN a.po_count IS NOT NULL THEN vp.valid_from END          AS price_valid_from,
# MAGIC          CASE WHEN a.po_count IS NOT NULL THEN vp.valid_to END            AS price_valid_to,
# MAGIC          CASE WHEN NOT (a.po_count IS NOT NULL) OR vp.net_price IS NULL THEN NULL
# MAGIC               WHEN vp.valid_to < current_date()   THEN 'Expired'
# MAGIC               WHEN vp.valid_from > current_date() THEN 'Not yet valid'
# MAGIC               WHEN vp.valid_from IS NULL OR vp.valid_to IS NULL THEN 'Validity unknown'
# MAGIC               ELSE 'Valid today' END                      AS price_validity,
# MAGIC          CASE WHEN a.po_count IS NOT NULL AND vp.net_price IS NOT NULL THEN
# MAGIC               CONCAT(vp.currency, ' ', format_number(vp.net_price, 2), ' per ', format_number(vp.price_unit, 0), ' ', vp.order_price_unit,
# MAGIC                      CASE WHEN vp.price_unit <> 1 THEN CONCAT(' (', format_number(vp.price_per_unit, 2), ' per ', vp.order_price_unit, ')') ELSE '' END)
# MAGIC               END                                         AS price_text,
# MAGIC          CASE WHEN a.po_count IS NOT NULL THEN vp.lead_time_days END      AS lead_time_days,
# MAGIC          CASE WHEN a.po_count IS NOT NULL THEN vp.standard_po_qty END     AS standard_po_qty,
# MAGIC          CASE WHEN a.po_count IS NOT NULL THEN vp.source_list END         AS source_list,
# MAGIC          CASE WHEN a.po_count IS NOT NULL THEN vp.over_delivery_tolerance_pct END AS over_delivery_tolerance_pct,
# MAGIC          CASE
# MAGIC            WHEN a.mpn IS NULL                                  THEN NULL
# MAGIC            WHEN a.qual_due IS NULL OR a.date_suspect           THEN 'INELIGIBLE'
# MAGIC            WHEN a.qual_due <  current_date()                   THEN 'EXPIRED'
# MAGIC            WHEN a.qual_due <= date_add(current_date(), 90)     THEN 'EXPIRING'
# MAGIC            ELSE 'VALID'
# MAGIC          END AS qual_status,
# MAGIC          d.specification_no, d.edition_no, d.effective_date AS spec_effective_date, d.pdf_manufacturer,
# MAGIC          COALESCE(d.n_lines > 0, FALSE) AS has_spec
# MAGIC   FROM silver.dim_pr_open_pkg p
# MAGIC   LEFT JOIN silver.dim_avl_vendor a ON a.item_code = p.material
# MAGIC   LEFT JOIN vp ON vp.pr_line = p.pr_line AND vp.mpn = a.mpn AND vp.supplier_code <=> a.supplier_code
# MAGIC   LEFT JOIN silver.dim_spec_doc d   ON d.mpn = a.mpn AND d.is_latest AND d.mpn <> ''
# MAGIC ),
# MAGIC roled AS (
# MAGIC   SELECT *,
# MAGIC     CASE
# MAGIC       WHEN last_po_date >= add_months(current_date(), -12)                   THEN 'CURRENT'
# MAGIC       WHEN last_po_date IS NOT NULL AND qual_status IN ('VALID', 'EXPIRING') THEN 'HISTORICAL'
# MAGIC       WHEN last_po_date IS NOT NULL                                           THEN 'HISTORICAL_NOT_QUALIFIED'
# MAGIC       WHEN mpn IS NOT NULL                                                    THEN 'ALTERNATIVE_UNUSED'
# MAGIC     END AS vendor_role
# MAGIC   FROM opts
# MAGIC ),
# MAGIC ranked AS (
# MAGIC   SELECT *,
# MAGIC     CASE WHEN last_po_date IS NOT NULL THEN
# MAGIC       DENSE_RANK() OVER (PARTITION BY pr_line, (last_po_date IS NOT NULL)
# MAGIC                          ORDER BY last_po_date DESC, po_count DESC, mpn)
# MAGIC     END AS last_used_rank
# MAGIC   FROM roled
# MAGIC ),
# MAGIC qualified_counts AS (
# MAGIC   SELECT pr_line,
# MAGIC     COUNT(DISTINCT CASE WHEN qual_status IN ('VALID','EXPIRING') THEN mpn END) AS n_qualified,
# MAGIC     COUNT(DISTINCT CASE WHEN qual_status = 'INELIGIBLE' THEN mpn END) AS n_ineligible
# MAGIC   FROM roled
# MAGIC   GROUP BY pr_line
# MAGIC )
# MAGIC SELECT r.*,
# MAGIC   qc.n_qualified,
# MAGIC   qc.n_ineligible,
# MAGIC   CASE
# MAGIC     WHEN r.mpn IS NULL       THEN 'NO_AVL_VENDOR'
# MAGIC     WHEN qc.n_qualified = 0  THEN 'NO_QUALIFIED_VENDOR'
# MAGIC     WHEN qc.n_qualified = 1  THEN 'SINGLE_SOURCE'
# MAGIC     ELSE                          'MULTI_SOURCE'
# MAGIC   END AS source_type
# MAGIC FROM ranked r
# MAGIC LEFT JOIN qualified_counts qc ON qc.pr_line = r.pr_line;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.v_vendor_spec AS
# MAGIC WITH vend AS (
# MAGIC   SELECT pr_line, mpn, MAX(material) AS material, MAX(manufacturer_name) AS manufacturer_name,
# MAGIC          MAX_BY(qual_status, qual_due) AS qual_status
# MAGIC   FROM gold.v_vendor_options WHERE mpn IS NOT NULL GROUP BY pr_line, mpn
# MAGIC )
# MAGIC SELECT DISTINCT v.pr_line, v.material, v.mpn, v.manufacturer_name, v.qual_status,
# MAGIC        d.specification_no, d.edition_no, p.test_seq, p.seg_no, p.parameter, p.component,
# MAGIC        CONCAT_WS(' ', p.parameter, CASE WHEN p.component IS NULL THEN NULL ELSE CONCAT('(', p.component, ')') END) AS parameter_label,
# MAGIC        CASE WHEN p.parse_status LIKE 'PARSED_%' THEN 'NUMERIC' ELSE 'TEXT' END AS spec_type,
# MAGIC        CASE WHEN p.parse_status LIKE 'PARSED_%' THEN
# MAGIC               CONCAT_WS(' ',
# MAGIC                 CASE WHEN p.spec_min IS NULL THEN CONCAT('max ', p.spec_max)
# MAGIC                      WHEN p.spec_max IS NULL THEN CONCAT('min ', p.spec_min)
# MAGIC                      WHEN p.spec_min = p.spec_max THEN CAST(p.spec_min AS STRING)
# MAGIC                      ELSE CONCAT(p.spec_min, ' - ', p.spec_max) END, p.unit)
# MAGIC             ELSE p.spec_line END AS spec_text
# MAGIC FROM vend v
# MAGIC JOIN silver.dim_spec_doc d ON d.mpn = v.mpn AND d.is_latest AND d.mpn <> ''
# MAGIC JOIN silver.dim_spec_test_parsed p ON p.specification_no = d.specification_no AND p.edition_no = d.edition_no
# MAGIC WHERE p.parse_status NOT IN ('NO_SPEC_TEXT', 'SKIPPED_SUMMARY_ROW') AND p.spec_line IS NOT NULL AND p.parameter IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- v_pr_tat : turnaround time per PR line in business days (Monday-Friday, no holiday table), from the start date up to, not including, today.
# MAGIC -- Start = PR final release date (requisition date if blank), stored in silver. Partially ordered lines keep running.
# MAGIC -- remaining = standard - elapsed (negative = over the standard). Missed when elapsed > standard; Risk when remaining <= risk limit; else Within.
# MAGIC CREATE OR REPLACE VIEW gold.v_pr_tat AS
# MAGIC WITH t AS (
# MAGIC   SELECT pr_line, pr, category, subcategory, tat_start_date, tat_start_source, tat_standard_days,
# MAGIC          tat_risk_max_remaining, tat_missed_bucket1_max_elapsed,
# MAGIC          CASE WHEN tat_start_date >= current_date() THEN 0
# MAGIC               ELSE size(filter(sequence(tat_start_date, date_sub(current_date(), 1), INTERVAL 1 DAY), d -> dayofweek(d) NOT IN (1, 7))) END AS tat_elapsed_days
# MAGIC   FROM silver.dim_pr_open_pkg
# MAGIC )
# MAGIC SELECT pr_line, pr, category, subcategory, tat_start_date, tat_start_source, tat_standard_days, tat_elapsed_days,
# MAGIC        tat_standard_days - tat_elapsed_days AS tat_remaining_days,
# MAGIC        CASE WHEN tat_elapsed_days > tat_standard_days                           THEN 'Missed'
# MAGIC             WHEN tat_standard_days - tat_elapsed_days <= tat_risk_max_remaining THEN 'Risk'
# MAGIC             ELSE 'Within' END AS tat_status,
# MAGIC        CASE WHEN tat_elapsed_days <= tat_standard_days THEN NULL
# MAGIC             WHEN tat_elapsed_days <= tat_missed_bucket1_max_elapsed
# MAGIC                  THEN CONCAT(tat_standard_days + 1, '-', tat_missed_bucket1_max_elapsed, ' days')
# MAGIC             ELSE CONCAT('>', tat_missed_bucket1_max_elapsed, ' days') END AS tat_missed_bucket
# MAGIC FROM t;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- v_pr_exceptions : one row per PR line x exception, with a plain-language reason. Seven types: the six existing ones plus L1_RELEASE_MISSING.
# MAGIC -- VENDOR_WITHOUT_SPEC ignores EXPIRED vendors (client instruction). INELIGIBLE vendors are counted.
# MAGIC CREATE OR REPLACE VIEW gold.v_pr_exceptions AS
# MAGIC WITH line AS (
# MAGIC   SELECT o.pr_line, MAX(o.pr) AS pr, MAX(o.pr_item) AS pr_item, MAX(o.plant) AS plant,
# MAGIC          MAX(o.pack_category) AS pack_category, MAX(o.pr_date) AS pr_date,
# MAGIC          MAX(o.material) AS material, MAX(o.short_text) AS material_name,
# MAGIC          MAX(o.source_type) AS source_type, COUNT(DISTINCT o.mpn) AS approved_vendors,
# MAGIC          MAX(b.buyer) AS buyer, MAX(b.l1_release_missing) AS l1_release_missing, MAX(b.final_release_date) AS final_release_date
# MAGIC   FROM gold.v_vendor_options o JOIN silver.dim_pr_open_pkg b ON b.pr_line = o.pr_line
# MAGIC   GROUP BY o.pr_line
# MAGIC ),
# MAGIC q AS (
# MAGIC   SELECT pr_line, array_join(array_sort(collect_set(manufacturer_name)), ', ') AS names
# MAGIC   FROM gold.v_vendor_options WHERE qual_status IN ('VALID', 'EXPIRING') GROUP BY pr_line
# MAGIC ),
# MAGIC ex AS (
# MAGIC   SELECT pr_line,
# MAGIC          array_join(array_sort(collect_set(CONCAT(manufacturer_name, ' (expired ', date_format(qual_due, 'yyyy-MM-dd'), ')'))), ', ') AS names
# MAGIC   FROM gold.v_vendor_options WHERE vendor_role = 'CURRENT' AND qual_status = 'EXPIRED' GROUP BY pr_line
# MAGIC ),
# MAGIC ns AS (
# MAGIC   SELECT pr_line, array_join(array_sort(collect_set(manufacturer_name)), ', ') AS names
# MAGIC   FROM gold.v_vendor_options WHERE mpn IS NOT NULL AND NOT has_spec AND qual_status <> 'EXPIRED' GROUP BY pr_line
# MAGIC ),
# MAGIC nh AS (
# MAGIC   SELECT pr_line FROM gold.v_vendor_options GROUP BY pr_line
# MAGIC   HAVING MAX(CASE WHEN mpn IS NOT NULL THEN 1 ELSE 0 END) = 1
# MAGIC      AND MAX(CASE WHEN po_count IS NOT NULL THEN 1 ELSE 0 END) = 0
# MAGIC ),
# MAGIC allx AS (
# MAGIC   SELECT l.pr_line, 'NO_AVL_VENDOR' AS exception,
# MAGIC          CONCAT('No approved vendor in the AVL for material ', l.material) AS reason
# MAGIC   FROM line l WHERE l.source_type = 'NO_AVL_VENDOR'
# MAGIC   UNION ALL
# MAGIC   SELECT l.pr_line, 'NO_QUALIFIED_VENDOR',
# MAGIC          CONCAT(l.approved_vendors, ' approved vendor(s), none with a valid qualification')
# MAGIC   FROM line l WHERE l.source_type = 'NO_QUALIFIED_VENDOR'
# MAGIC   UNION ALL
# MAGIC   SELECT l.pr_line, 'SINGLE_SOURCE', CONCAT('Only one qualified vendor: ', q.names)
# MAGIC   FROM line l JOIN q ON q.pr_line = l.pr_line WHERE l.source_type = 'SINGLE_SOURCE'
# MAGIC   UNION ALL
# MAGIC   SELECT l.pr_line, 'CURRENT_VENDOR_EXPIRED', CONCAT('Current vendor qualification expired: ', ex.names)
# MAGIC   FROM line l JOIN ex ON ex.pr_line = l.pr_line
# MAGIC   UNION ALL
# MAGIC   SELECT l.pr_line, 'NO_HISTORY_AVL_EXISTS',
# MAGIC          CONCAT(l.approved_vendors, ' approved vendor(s) but no purchase order history')
# MAGIC   FROM line l JOIN nh ON nh.pr_line = l.pr_line
# MAGIC   UNION ALL
# MAGIC   SELECT l.pr_line, 'VENDOR_WITHOUT_SPEC', CONCAT('No specification samwed pdf file available for: ', ns.names)
# MAGIC   FROM line l JOIN ns ON ns.pr_line = l.pr_line
# MAGIC   UNION ALL
# MAGIC   SELECT l.pr_line, 'L1_RELEASE_MISSING',
# MAGIC          CONCAT('PR L1 release date is missing', CASE WHEN l.final_release_date IS NULL THEN '' ELSE CONCAT(' (final release ', date_format(l.final_release_date, 'yyyy-MM-dd'), ')') END)
# MAGIC   FROM line l WHERE l.l1_release_missing
# MAGIC )
# MAGIC SELECT l.pr, l.pr_line, l.pr_item, l.plant, l.pack_category, l.pr_date, l.material, l.material_name, l.buyer,
# MAGIC        a.exception, a.reason
# MAGIC FROM allx a JOIN line l ON l.pr_line = a.pr_line;
# MAGIC
# MAGIC ------------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Gold patch: action_status now really means exceptions (option 2).
# MAGIC -- Replace the whole v_pr_summary cell (CELL 9) in Gold_Vendor_Discovery_NB with this. Nothing else in gold has to change:
# MAGIC -- v_pr_list and v_buyer_summary already read 'No_Exception' / 'Has_Exceptions' from this view.
# MAGIC --   No_Exception   = the PR line has 0 exceptions
# MAGIC --   Has_Exceptions = the PR line has at least one exception (any of the seven types)
# MAGIC -- The column non_action_reason is replaced by exception_reasons (all reasons of the line, joined). A PR is No_Exception only when every line is.
# MAGIC
# MAGIC CREATE OR REPLACE VIEW v_pr_summary AS
# MAGIC WITH v AS (
# MAGIC   SELECT pr_line,
# MAGIC          COUNT(DISTINCT mpn)                              AS approved_vendors,
# MAGIC          MAX(n_qualified)                                 AS qualified_vendors,
# MAGIC          MAX(n_ineligible)                                AS ineligible_vendors,
# MAGIC          COUNT(DISTINCT CASE WHEN has_spec THEN mpn END)  AS vendors_with_spec,
# MAGIC          MAX(source_type)                                 AS source_type
# MAGIC   FROM gold.v_vendor_options
# MAGIC   GROUP BY pr_line
# MAGIC ),
# MAGIC e AS (
# MAGIC   SELECT pr_line, array_sort(collect_set(exception)) AS exception_list, COUNT(*) AS exception_count,
# MAGIC          array_join(array_sort(collect_set(reason)), ' | ') AS exception_reasons
# MAGIC   FROM gold.v_pr_exceptions GROUP BY pr_line
# MAGIC )
# MAGIC SELECT p.pr_line, p.pr, p.pr_item, p.plant, p.pack_category, p.category, p.subcategory, p.pr_date, p.pr_status, p.material,
# MAGIC        p.short_text AS material_name, p.qty, p.uom, p.buyer,
# MAGIC        t.tat_status, t.tat_elapsed_days, t.tat_remaining_days, t.tat_missed_bucket, t.tat_start_date,
# MAGIC        COALESCE(v.approved_vendors, 0)   AS approved_vendors,
# MAGIC        COALESCE(v.qualified_vendors, 0)  AS qualified_vendors,
# MAGIC        COALESCE(v.ineligible_vendors, 0) AS ineligible_vendors,
# MAGIC        COALESCE(v.vendors_with_spec, 0)  AS vendors_with_spec,
# MAGIC        v.source_type,
# MAGIC        array_join(e.exception_list, ', ') AS exceptions,
# MAGIC        e.exception_list,
# MAGIC        COALESCE(e.exception_count, 0)    AS exception_count,
# MAGIC        CASE WHEN COALESCE(e.exception_count, 0) = 0 THEN 'No_Exception' ELSE 'Has_Exceptions' END AS action_status,
# MAGIC        e.exception_reasons
# MAGIC FROM silver.dim_pr_open_pkg p
# MAGIC LEFT JOIN v ON v.pr_line = p.pr_line
# MAGIC LEFT JOIN e ON e.pr_line = p.pr_line
# MAGIC LEFT JOIN gold.v_pr_tat t ON t.pr_line = p.pr_line;
# MAGIC
# MAGIC -- Check: every line is in exactly one group, and the groups follow the exception count.
# MAGIC -- SELECT action_status, COUNT(*) AS lines, MIN(exception_count) AS min_exc, MAX(exception_count) AS max_exc FROM gold.v_pr_summary GROUP BY 1;
# MAGIC -- Expect: No_Exception -> min 0 / max 0 ; Has_Exceptions -> min >= 1. Since 35 lines have L1 missing and many have vendors without specs,
# MAGIC -- most lines will be Has_Exceptions (this is expected).
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- v_pr_list : one row per PR (landing table). PR date = EARLIEST line date. NEW = created in the last 7 days, else BACKLOG.
# MAGIC -- Existing columns unchanged; added buyer, category, TAT roll-up and action / non action.
# MAGIC CREATE OR REPLACE VIEW gold.v_pr_list AS
# MAGIC SELECT s.pr,
# MAGIC        MIN(s.pr_date)                                              AS pr_date,
# MAGIC        DATEDIFF(current_date(), MIN(s.pr_date))                    AS age_days,
# MAGIC        CASE WHEN MIN(s.pr_date) >= date_sub(current_date(), 7) THEN 'NEW' ELSE 'BACKLOG' END AS priority,
# MAGIC        MAX(s.plant)                                                AS plant,
# MAGIC        array_join(array_sort(collect_set(s.buyer)), ', ')          AS buyer,
# MAGIC        MAX(s.category)                                             AS category,
# MAGIC        array_join(array_sort(collect_set(s.subcategory)), ', ')    AS subcategory,
# MAGIC        COUNT(*)                                                    AS lines,
# MAGIC        COUNT(DISTINCT s.material)                                  AS materials,
# MAGIC        SUM(CASE WHEN s.pr_status = 'OPEN' THEN 1 ELSE 0 END)              AS open_lines,
# MAGIC        SUM(CASE WHEN s.pr_status = 'PARTIALLY_ORDERED' THEN 1 ELSE 0 END) AS partially_ordered_lines,
# MAGIC        CASE WHEN SUM(CASE WHEN s.tat_status = 'Missed' THEN 1 ELSE 0 END) > 0 THEN 'Missed'
# MAGIC             WHEN SUM(CASE WHEN s.tat_status = 'Risk'   THEN 1 ELSE 0 END) > 0 THEN 'Risk' ELSE 'Within' END AS tat_status,
# MAGIC        SUM(CASE WHEN s.tat_status = 'Missed' THEN 1 ELSE 0 END)    AS tat_missed_lines,
# MAGIC        SUM(CASE WHEN s.tat_status = 'Risk'   THEN 1 ELSE 0 END)    AS tat_risk_lines,
# MAGIC        SUM(CASE WHEN s.tat_status = 'Within' THEN 1 ELSE 0 END)    AS tat_within_lines,
# MAGIC        MAX(s.tat_elapsed_days)                                     AS tat_max_elapsed_days,
# MAGIC        MIN(s.tat_remaining_days)                                   AS tat_min_remaining_days,
# MAGIC        SUM(s.exception_count)                                      AS exception_count,
# MAGIC        SUM(CASE WHEN s.exception_count > 0 THEN 1 ELSE 0 END)     AS lines_with_exceptions,
# MAGIC        array_join(array_sort(array_distinct(flatten(collect_list(s.exception_list)))), ', ') AS exceptions,
# MAGIC        CASE WHEN SUM(s.exception_count) > 0 THEN 'Has exceptions' ELSE 'No exceptions' END AS exception_flag,
# MAGIC        SUM(CASE WHEN s.action_status = 'No_Exception' THEN 1 ELSE 0 END)     AS action_lines,
# MAGIC        SUM(CASE WHEN s.action_status = 'Has_Exceptions' THEN 1 ELSE 0 END) AS non_action_lines,
# MAGIC        CASE WHEN SUM(CASE WHEN s.action_status = 'Has_Exceptions' THEN 1 ELSE 0 END) = 0 THEN 'No_Exception' ELSE 'Has_Exceptions' END AS action_status
# MAGIC FROM gold.v_pr_summary s
# MAGIC GROUP BY s.pr;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- v_buyer_summary : one row per buyer (PR creator). Entry point for the buyer -> PRs -> materials storyline.
# MAGIC CREATE OR REPLACE VIEW gold.v_buyer_summary AS
# MAGIC WITH pr AS (
# MAGIC   SELECT s.buyer, s.pr, l.priority, l.action_status, l.pr_date
# MAGIC   FROM (SELECT DISTINCT buyer, pr FROM gold.v_pr_summary) s JOIN gold.v_pr_list l ON l.pr = s.pr
# MAGIC ),
# MAGIC prc AS (
# MAGIC   SELECT buyer, COUNT(DISTINCT pr) AS prs,
# MAGIC          COUNT(DISTINCT CASE WHEN priority = 'NEW' THEN pr END)             AS new_prs,
# MAGIC          COUNT(DISTINCT CASE WHEN priority = 'BACKLOG' THEN pr END)         AS backlog_prs,
# MAGIC          COUNT(DISTINCT CASE WHEN action_status = 'No_Exception' THEN pr END)     AS action_prs,
# MAGIC          COUNT(DISTINCT CASE WHEN action_status = 'Has_Exceptions' THEN pr END) AS non_action_prs,
# MAGIC          MIN(pr_date) AS oldest_pr_date
# MAGIC   FROM pr GROUP BY buyer
# MAGIC ),
# MAGIC ln AS (
# MAGIC   SELECT buyer, COUNT(*) AS lines,
# MAGIC          SUM(CASE WHEN tat_status = 'Missed' THEN 1 ELSE 0 END) AS tat_missed_lines,
# MAGIC          SUM(CASE WHEN tat_status = 'Risk'   THEN 1 ELSE 0 END) AS tat_risk_lines,
# MAGIC          SUM(CASE WHEN tat_status = 'Within' THEN 1 ELSE 0 END) AS tat_within_lines,
# MAGIC          SUM(CASE WHEN action_status = 'No_Exception'     THEN 1 ELSE 0 END) AS action_lines,
# MAGIC          SUM(CASE WHEN action_status = 'Has_Exceptions' THEN 1 ELSE 0 END) AS non_action_lines,
# MAGIC          SUM(CASE WHEN exception_count > 0 THEN 1 ELSE 0 END) AS lines_with_exceptions,
# MAGIC          MAX(tat_elapsed_days) AS tat_max_elapsed_days
# MAGIC   FROM gold.v_pr_summary GROUP BY buyer
# MAGIC )
# MAGIC SELECT prc.*, ln.lines, ln.tat_missed_lines, ln.tat_risk_lines, ln.tat_within_lines,
# MAGIC        ln.action_lines, ln.non_action_lines, ln.lines_with_exceptions, ln.tat_max_elapsed_days
# MAGIC FROM prc JOIN ln ON ln.buyer <=> prc.buyer;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Checks (expected on the current extract, to 2026-11-12): 170 PR lines in 23 PRs.
# MAGIC SELECT COUNT(*) AS lines, COUNT(DISTINCT pr) AS prs FROM gold.v_pr_summary;
# MAGIC -- TAT: expect Missed 40 (4 in 6-10 days, 36 over 10), Risk 130, Within 0 (drifts as days pass)
# MAGIC SELECT tat_status, tat_missed_bucket, COUNT(*) AS lines FROM gold.v_pr_tat GROUP BY 1, 2 ORDER BY 1, 2;
# MAGIC -- Exceptions: L1_RELEASE_MISSING should be 35
# MAGIC SELECT exception, COUNT(*) AS lines FROM gold.v_pr_exceptions GROUP BY 1 ORDER BY 1;
# MAGIC -- Action / non action: expect ACTION about 15 lines
# MAGIC SELECT action_status, COUNT(*) AS lines FROM gold.v_pr_summary GROUP BY 1;
# MAGIC -- Price: purchased vendors only. Expect a status for every purchased pair; non-purchased vendors show NULL.
# MAGIC SELECT price_status, price_validity, COUNT(DISTINCT CONCAT(pr_line, '|', mpn)) AS pr_line_mpn_pairs FROM gold.v_vendor_options WHERE mpn IS NOT NULL GROUP BY 1, 2 ORDER BY 1, 2;
# MAGIC SELECT pr_line, manufacturer_name, vendor_role, price_text, price_validity, lead_time_days, source_list, last_po_qty, last_po_delivery_date FROM gold.v_vendor_options WHERE price_status IS NOT NULL ORDER BY pr_line LIMIT 20;
# MAGIC -- Buyers: expect 8
# MAGIC SELECT * FROM gold.v_buyer_summary ORDER BY prs DESC;
