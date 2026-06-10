# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Single-file containerized batch job that copies tables from PostgreSQL or Oracle into BigQuery using [dlt](https://dlthub.com/). Entirely configured through environment variables — no CLI args, no config files. Designed to run as a Cloud Run Job or Kubernetes Job. All logic lives in `main.py`; `run_pipeline()` is the entrypoint.

## Commands

```bash
# Local run (needs .env + a Service Account creds JSON, see docker-compose.yml)
docker-compose up --build

# Dependency management (uv, Python 3.13)
uv sync                    # install/lock deps
uv run python main.py      # run pipeline directly against current env
```

There are no tests, linters, or build steps beyond Docker. CI (`.github/workflows/build-push.yml`) only builds and pushes the image to GCP on push to `main` / `v*` tags via a shared reusable workflow.

## Configuration surface

The full env-var reference lives in `README.md` — read it before changing config-handling code. Required: `DB_URL_SECRET` (Secret Manager resource name holding the SQLAlchemy URL) and `BQ_DATASET_ID`. The connection URL is fetched from Secret Manager at runtime, never passed directly.

`TABLE_CONFIGS` (JSON) is the main per-table control: `incremental`, `primary_key`, `write_disposition`, `partition`, `cluster`, `include`/`exclude` (columns), `on_cursor_value_missing`, and `columns` (per-column dlt hints like `data_type: wei`). `TABLE_QUERIES` (JSON) maps a table name to a raw SQL string for fully manual extraction.

## Architecture & non-obvious behavior

Read order matters in `main.py` — logging is configured first (before any GCP import) so Cloud Run trace correlation works.

**Two table paths.** Standard tables go through dlt's `sql_database` source with SQLAlchemy reflection. Tables listed in `TABLE_QUERIES` are built as **manual `@dlt.resource` generators** (`make_manual_resource`) and explicitly *excluded* from reflection-based discovery — this prevents dlt from reflecting (and copying) unwanted/sensitive columns. The source is cloned with `with_name=f"source_{bq_dataset_id}"` to force dlt to ignore any previously-persisted polluted schema.

**Monkey patches at module load — do not remove without understanding why:**
- `oracledb.defaults.fetch_lobs = False` + a global Oracle output-type handler (`date_out_converter`) that neutralizes out-of-range Oracle dates (e.g. year -5579) to `NULL`. Raw LOBs and bad dates both crash pyarrow otherwise.
- The Oracle handler is reinstalled via SQLAlchemy `connect` / `before_cursor_execute` events, gated on `hasattr(conn, "outputtypehandler")` so it never touches PostgreSQL connections.
- **Patch 1**: wraps `BigQuerySqlClient.truncate_tables` so a `replace` load against a manually-deleted table warns instead of crashing (dlt recreates it).
- `table_adapter_callback` and forced `nullable=True` hints on every column — Arrow crashes on non-nullable columns during type promotion. `SOURCES__SQL_DATABASE__ARROW_CONCAT_PROMOTE_OPTIONS=full` is set for the same reason.

**Column comments → BQ descriptions.** Before the per-resource hints loop, source column comments are fetched once and keyed by `naming.normalize_identifier` into `col_comments`: Oracle from `ALL_COL_COMMENTS` (owner = `DB_SCHEMA` or session user), PostgreSQL from `pg_description` joined via `information_schema.columns`. They're merged as `description` into `force_null_hints` with `setdefault`, so an explicit `columns.description` in `TABLE_CONFIGS` wins. The fetch is dialect-branched (`"oracle"`/`"postgres"` in the URL), wrapped in try/except (non-blocking), and only reaches reflected columns — `TABLE_QUERIES` resources get none.

**Column include/exclude** is applied two ways simultaneously: an `add_map` filter strips data (handles both dict and `pyarrow.Table` items) *and* the column is deleted from `res.columns` so it never reaches the BQ schema. `include` is a whitelist computed as the complement against reflected columns — it does **not** work for `TABLE_QUERIES` resources (no reflected schema); filter in the SQL instead.

**State reset gotcha** (commented in `main.py` near line 117): to fully reset dlt, drop the *entire* BigQuery dataset. Never delete only `_dlt_pipeline_state` / `_dlt_loads` — dlt won't recreate them and will crash.

**Connection pool** sizing is env-driven (`DB_POOL_SIZE` etc.). Rule of thumb: `DB_POOL_SIZE >= EXTRACT__MAX_PARALLEL_ITEMS` with headroom for reflection, or you hit `QueuePool limit reached` on wide databases.

## Conventions

Code comments and logs are in **French** — match that when editing. The codebase is one file by design; keep it that way unless there's a strong reason to split.
