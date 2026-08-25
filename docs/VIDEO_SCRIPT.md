# Video Walkthrough Outline (5–10 minutes)

Use this as a talking-points script. Record your screen (Databricks
workspace + repo) while you talk through each section.

## 1. Intro (30 sec)
- What the project is: a generic, metadata-driven file ingestion framework
  for Databricks/Delta Lake.
- The problem it solves: avoid writing a new notebook per source file/feed.

## 2. Repo structure tour (1 min)
Open the repo and briefly show:
- `Ingestion_framework/01_Setup_Framework_Tables.py` — one-time setup
- `Ingestion_framework/Ingestion_Framework.py` — the reusable driver
- `config/config_file_sample.csv` — example source registrations
- `databricks.yml` + `resources/framework_job.yml` — how it's deployed as a job

## 3. The control tables (1–2 min)
- Show `ingestion_config`: explain each column briefly (source path, file
  pattern, format, delimiter, target table, load type, archive/error paths,
  active flag).
- Show `ingestion_audit`: explain it's the run history — one row per file
  per run, with status, record counts, and error messages.
- Emphasize: **adding a new source = adding a row, not writing code.**

## 4. Setup notebook walkthrough (1–2 min)
- Creates the `framework` schema and both Delta tables.
- Reads the config CSV, validates required fields and `load_type` values.
- Uses a Delta `MERGE` to upsert config rows by `config_id` — safe to re-run.

## 5. Driver notebook walkthrough (2–3 min)
- Reads active configs.
- For each config: finds matching files by glob pattern.
- For each file: checks the audit table for idempotency (skip if already
  succeeded), reads with the configured options, tags it with lineage
  columns, writes to the Delta target (append or overwrite), writes a
  SUCCESS audit row, and archives the file.
- On failure: writes a FAILED audit row and moves the file to the error
  path — the run keeps going for the rest of the files/configs.
- Point out the final cells that display the current run's audit trail and
  the resulting target tables.

## 6. Live run demo (1–2 min)
- Trigger `framework_job` (via `databricks bundle run framework_job` or
  the Jobs UI).
- Show the audit table populate in real time / after the run.
- Show a target Delta table with the loaded data and lineage columns.

## 7. Deployment (30 sec–1 min)
- Show `databricks bundle validate` / `databricks bundle deploy`.
- Mention `databricks.yml` targets (dev/prod) and that the job path matches
  the notebook path in the workspace.

## 8. Wrap-up (30 sec)
- Recap: config-driven, idempotent, auditable, self-healing on failure.
- Where you'd take it next (e.g. parameterizing the config file path,
  adding schema evolution alerts, notifications on FAILED rows).
