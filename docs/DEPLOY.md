# Deploying the RCM Lakehouse to Databricks

The `databricks/` folder is a **Databricks Asset Bundle** (`databricks.yml`). Deploying it
builds the shared `common` helpers into a wheel, uploads the notebooks, and installs the wheel
on the job clusters so `from common import ...` resolves at runtime.

> Pushing to GitHub does **not** update the workspace. You must run `databricks bundle deploy`
> from an authenticated machine for changes to take effect — especially changes to
> `databricks/common/*` (the `rcm-common` wheel), which only refresh when the wheel is rebuilt.

## Target workspace

| Setting | Value |
| --- | --- |
| Workspace host | `https://adb-7405606479280392.12.azuredatabricks.net` (Azure) |
| Catalog | `rcm` |
| Storage account | `strcmdemo70648c` |

## 1. Prerequisites (one-time)

```bash
# bundle-capable Databricks CLI (NOT the old pip `databricks-cli`, which has no `bundle`)
winget install Databricks.DatabricksCLI        # macOS: brew install databricks/tap/databricks

# python build backend, used by the wheel artifact at deploy time
pip install build

# authenticate to the AZURE RCM workspace under its own profile so it does not collide
# with any other profile already in ~/.databrickscfg
databricks auth login \
  --host https://adb-7405606479280392.12.azuredatabricks.net \
  --profile rcm-azure
```

## 2. Deploy

```bash
cd databricks
databricks bundle validate -t dev -p rcm-azure
databricks bundle deploy   -t dev -p rcm-azure
```

This builds `dist/rcm_common-<version>-py3-none-any.whl` (version from `databricks/pyproject.toml`),
uploads all notebooks, and attaches the wheel to every job task.

> **Bump the wheel version** in `databricks/pyproject.toml` whenever `common/*` changes, so the
> cluster reliably reinstalls it instead of skipping an identical version.

## 3. Verify the new code is on the cluster

Run in a notebook on the deployed cluster:

```python
import common.scd as scd
print(hasattr(scd, "row_hash_expr"))   # True  -> new wheel is installed
```

If this prints `False`, the cluster still has an old wheel — bump the version and redeploy,
or detach/reattach (restart) the cluster to force reinstall.

## 4. SCD2 hash migration (one-time, only if dimensions already exist)

`scd.scd2_merge` uses `xxhash64` for change detection. If `silver.patient` / `silver.provider` /
`silver.department` were built with the older `sha2` hash, run the backfill **once, before** the
next silver run — otherwise every member is spuriously re-versioned on first run.

```
run once:  databricks/migrations/2026_06_backfill_row_hash_xxhash64.py
```

It is idempotent and self-guards (prints `SKIP` for tables that don't exist yet). On a brand-new
environment there is nothing to migrate — skip this step.

## 5. Run the pipeline

```
migration (step 4, if applicable)
  -> silver SCD2 notebooks: 01_silver_patient.py, 02_silver_provider_department.py
  -> remaining silver / gold notebooks, or just trigger the "RCM Batch Pipeline" job
```

Sanity check after the first silver run (should be a no-op for unchanged members):

```python
spark.table("rcm.silver.patient").filter("is_current = true").count()   # unchanged vs prior run
```
