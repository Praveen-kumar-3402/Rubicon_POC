# Rubicon vendor discovery - CI/CD demo (GitHub + Databricks Asset Bundles)

One workspace, three environments, each its own Unity Catalog catalog:

| Environment | Catalog | Deployed by |
|---|---|---|
| DEV  | `rubicon_dev`  | every pull request (CI) |
| QA   | `rubicon_qa`   | merge to `main` (CD, automatic) |
| PROD | `rubicon_prod` | after QA passes **and** a reviewer approves (CD) |

```
developer branch -> pull request -> CI: syntax check, bundle validate, deploy to DEV, run pipeline + checks
                 -> merge to main -> CD: deploy + run in QA -> approval -> deploy + run in PROD
```
The pipeline is one job per environment: `00_setup` -> `01_bronze` -> `02_silver` -> `03_gold` -> `04_checks`.
A failed check fails the job, so a bad change never reaches the next environment. The same code runs everywhere; only the `catalog` parameter differs.

## One-time setup
1. **Repo**: push this folder to a GitHub repository (branch `main`).
2. **Catalogs**: `rubicon_dev`, `rubicon_qa`, `rubicon_prod` exist; the service principal needs USE CATALOG, CREATE SCHEMA, CREATE TABLE on each.
3. **Raw inputs**: the landing Volume (CSV/Excel) and the `samwed` spec tables are read from `rubicon_poc_workspace` (variable `source_catalog`). Change it in `databricks.yml` if each catalog gets its own copy.
4. **Service principal** with OAuth secret, with CAN USE on a SQL warehouse (the dashboard) and permission to run jobs.
5. **GitHub > Settings > Environments**: create `dev`, `qa`, `prod`. In each add
   - variables: `DATABRICKS_HOST` (workspace URL), `WAREHOUSE_ID`
   - secrets: `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`
   - on `prod`: **Required reviewers** (this is the approval gate).
6. **Branch protection** on `main`: require the CI check to pass before merge.

## Demo script
1. Create a branch, change a rule (for example the Opex TAT standard in `02_silver.py`), open a pull request -> CI deploys to `rubicon_dev` and runs the checks.
2. Show the green check on the PR and the data in `rubicon_dev`.
3. Merge -> `rubicon_qa` updates automatically.
4. Approve the pending `prod` deployment -> `rubicon_prod` updates.
5. Failure demo: break a rule so `04_checks` fails -> the PR is blocked / PROD is never reached.

## Local commands (optional)
```
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run vendor_discovery_pipeline -t dev
```
(set `BUNDLE_VAR_warehouse_id=<id>` first, or pass `--var warehouse_id=<id>`).

## Notes
- Notebooks are Databricks source-format `.py` files, generated from the current Bronze / Silver / Gold notebooks with the catalog turned into the `catalog` parameter, plus the exception-based `action_status` change.
- The dashboard copy in `dashboards/` has its table names without a catalog; the bundle sets the catalog per environment.
- Not yet run end-to-end: the first DEV run may surface small fixes.
=======
