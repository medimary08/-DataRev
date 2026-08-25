# Metadata-Driven Ingestion Framework (Databricks)

A generic, configuration-driven file ingestion framework for Databricks /
Delta Lake. Instead of writing a separate notebook per source, you register
each source as a **row** in a control table. A single reusable driver
notebook reads that control table, discovers matching files, loads them into
the right Delta table, records an audit trail, and archives (or quarantines)
each file it touches.

## Why this exists

- **One framework, many sources.** Add a new CSV/JSON/Parquet feed by adding
  a config row — no new code.
- **Idempotent.** Files that already succeeded (tracked in the audit table)
  are skipped on re-run.
- **Auditable.** Every file processed (success, failure, or skip) gets a row
  in `ingestion_audit` with record counts, timing, and error messages.
- **Self-healing on failure.** Failed files are moved to an `error_path`
  instead of sitting in the landing zone; successful files move to an
  `archive_path`.

## Project structure

```
ingestion-framework/
├── databricks.yml                          # Databricks Asset Bundle entry point
├── resources/
│   └── framework_job.yml                   # Job definition (the workflow that runs the framework)
├── Ingestion_framework/
│   ├── 01_Setup_Framework_Tables.py         # One-time setup: creates control tables + loads config CSV
│   └── Ingestion_Framework.py               # Main driver notebook: runs every active config each execution
├── config/
│   └── config_file_sample.csv               # Example ingestion_config rows
├── .gitignore
└── README.md
```

## How it works

### 1. Control tables (`workspace.framework` schema)

**`ingestion_config`** — one row per source feed:

| column | purpose |
|---|---|
| `config_id` | unique id for the source |
| `source_name` | friendly name, e.g. `CUSTOMERS` |
| `file_name_pattern` | glob pattern used to match files, e.g. `customers_*.csv` |
| `file_format` | `csv` \| `json` \| `parquet` |
| `delimiter_name` | `COMMA` \| `PIPE` \| `TAB` \| `SEMICOLON` (CSV only) |
| `header_flag` / `infer_schema_flag` | reader options |
| `source_path` | landing zone (Volume path) to scan |
| `target_catalog` / `target_schema` / `target_table` | Delta destination |
| `load_type` | `APPEND` or `OVERWRITE` |
| `archive_path` / `error_path` | where processed files are moved |
| `active_flag` | toggles the source on/off without deleting the row |

**`ingestion_audit`** — one row per file per run, written by the framework:
status (`SUCCESS` / `FAILED` / `SKIPPED`), record counts, timestamps, and
error message.

### 2. Setup notebook — `01_Setup_Framework_Tables.py`

Run once (and again whenever `config_file_sample.csv`, or your real config
file, changes):
1. Creates the `workspace.framework` schema.
2. Creates `ingestion_config` and `ingestion_audit` as Delta tables.
3. Reads a CSV of config rows and **merges** them into `ingestion_config`
   (upsert on `config_id`), with validation for required fields and allowed
   `load_type` values.

Update `CONFIG_FILE_PATH` in that notebook (or better, promote it to a job
parameter) to point at your real config CSV location, e.g. a Unity Catalog
Volume.

### 3. Driver notebook — `Ingestion_Framework.py`

For every row where `active_flag = true`:
1. Lists the `source_path` and matches files against `file_name_pattern`.
2. For each matching file:
   - Skips it if it already has a `SUCCESS` audit row (idempotency).
   - Reads it with the configured format/delimiter/options.
   - Adds lineage columns (`_source_file_name`, `_config_id`, `_run_id`,
     `_ingestion_timestamp`, etc.).
   - Writes it to the target Delta table (`APPEND` with schema merge, or
     `OVERWRITE` with schema overwrite).
   - Writes a `SUCCESS` audit row and moves the file to `archive_path`.
   - On any failure: writes a `FAILED` audit row (truncated error message)
     and moves the file to `error_path`, without stopping the rest of the
     run.
3. If a config has no matching files, or its `source_path` doesn't exist,
   it's recorded as `SKIPPED` and the framework moves on to the next config.
4. At the end, displays the current run's audit rows and the resulting
   target tables for verification.

## Deploying with Databricks Asset Bundles

This repo is a [Databricks Asset Bundle](https://docs.databricks.com/dev-tools/bundles/index.html).

```bash
# 1. Install the Databricks CLI (v0.205+) if you don't have it
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# 2. Authenticate
databricks auth login --host https://<your-workspace-instance>.cloud.databricks.com

# 3. From the repo root, validate the bundle
databricks bundle validate -t dev

# 4. Deploy (syncs notebooks + creates/updates the job)
databricks bundle deploy -t dev

# 5. Run the job
databricks bundle run framework_job -t dev
```

`resources/framework_job.yml` defines a single-task job, `framework_job`,
that runs `Ingestion_framework/Ingestion_Framework.py` as a notebook task.
Before the job's first run, execute `01_Setup_Framework_Tables.py` once
(manually, or as a preceding task/job) so the control tables exist and are
populated.

> Update the `host` and `run_as` / workspace path values in `databricks.yml`
> and `resources/framework_job.yml` to match your own workspace and user
> before deploying.

## Pushing to Git

```bash
cd ingestion-framework
git init
git add .
git commit -m "Initial commit: metadata-driven ingestion framework"
git branch -M main
git remote add origin <your-repo-url>
git push -u origin main
```

## Video walkthrough

See `docs/VIDEO_SCRIPT.md` for a suggested 5–10 minute walkthrough outline
covering the framework's structure, the config-driven design, and a live
run of the job.
