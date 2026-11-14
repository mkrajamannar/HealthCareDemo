# Architecture — Patient 360

## Goal

One trusted patient view from four source patterns (table, file, API, stream),
built on Azure + Databricks with a medallion (Bronze / Silver / Gold) lakehouse.

## Sources

| Source | System | Pattern | Frequency | Lands in |
|---|---|---|---|---|
| patients, admissions | Azure SQL Database | JDBC full + incremental by `admit_date` | Daily batch | Bronze |
| lab_results | CSV files | ADF copy to ADLS landing | Daily batch | Bronze |
| claims | JSON files | ADF copy to ADLS landing | Daily batch | Bronze |
| medications | FHIR R4 API | REST, paged, JSON | Daily batch | Bronze |
| vitals | Bedside monitors | Event Hubs -> Structured Streaming | Near-real time | Silver |

## Medallion layers (Delta Lake on ADLS Gen2)

- **Bronze — raw, append-only.** Exact source copy + `_load_timestamp`, `_source_file`,
  `_batch_id`. No cleaning, no dedupe. Schema enforced on read, not on write.
  One Delta table per source: `bronze_patients`, `bronze_admissions`, `bronze_lab_results`,
  `bronze_claims`, `bronze_medications`.
- **Silver — cleaned, conformed.** Typed columns, trimmed strings, standard date
  formats, deduped by business key, null/invalid rows quarantined to
  `silver_quarantine` with a reason code. Surrogate keys added. SCD Type 2 on
  `silver_patients` (address / physician can change). Streaming vitals land here
  after parsing + watermarking.
- **Gold — business-ready marts.** Star schema for Power BI:
  - `gold_dim_patient` (SCD2, one row per current patient)
  - `gold_fact_admissions` (one row per admission, LOS, readmission flags)
  - `gold_fact_lab_results` (abnormal flags)
  - `gold_fact_claims` (billed vs paid, denial flag)
  - `gold_fact_medications` (active meds per patient)
  - `gold_fact_vitals_hourly` (hourly aggregates from the stream)
  - `gold_patient_360` (one row per patient: latest admission, open claims,
    active meds, last vitals, readmission-risk score)

## Batch ingestion (Step 4)

- **Table:** Spark JDBC read from Azure SQL, or ADF Copy activity. Incremental:
  `WHERE admit_date > last watermark`. Watermark stored in a Delta control table.
- **File:** ADF Copy activity from landing container, or Databricks Auto Loader
  (`cloudFiles`) for incremental file discovery.
- **API:** Python `requests` with paging (`_count`, `next` link), retry with
  backoff, raw JSON saved to Bronze before any flattening. The demo bundle in
  `data/api/` is a captured FHIR searchset so the pipeline runs without a live server.

## Streaming (Step 6)

- Monitors publish vitals JSON to Event Hubs (one event per reading).
- Databricks Structured Streaming reads the hub, parses JSON with a fixed schema,
  applies a 10-minute watermark on `event_time`, and writes to `silver_vitals`.
- A second stream aggregates hourly per patient into `gold_fact_vitals_hourly`.
- Alerts: heart rate > 100 or < 60, or SpO2 < 93, written to `silver_vitals_alerts`.
- Local demo: the same code runs against `data/stream/vitals.jsonl` as a file
  stream, so it works without an Event Hub.

## Orchestration

Azure Data Factory pipelines (see `infra/`):

1. `pl_ingest_batch` — daily: table extract, file copy, API pull, then trigger
   the Bronze notebook.
2. `pl_transform_silver_gold` — runs after Bronze: Silver notebook, then Gold notebook.
3. `pl_stream_healthcheck` — hourly: checks streaming query lag and alert counts.

Databricks Jobs (or ADF -> Databricks activity) run the notebooks in order:
`01_bronze` -> `02_silver` -> `03_gold`.

## Data quality

- Bronze: row counts logged per source per batch; zero-row loads raise an alert.
- Silver: Great Expectations-style checks in the notebook — no null business keys,
  dates in range, `discharge_date >= admit_date`, billed >= paid. Failures go to
  `silver_quarantine` and the pipeline continues with valid rows.
- Gold: referential checks — every fact `patient_id` exists in `dim_patient`.

## Security and governance (PHI)

- Demo data is fully synthetic; no real patient data anywhere in the repo.
- In production: Purview scans + classifies PHI columns; column-level masking in
  Databricks SQL; secrets in Key Vault (never in notebooks); ADLS private
  endpoints; Unity Catalog for access control and audit.
- PII columns (`first_name`, `last_name`) are masked in the Gold layer for
  non-clinical roles.

## Cost note (demo)

Everything runs on a single-node Databricks cluster and serverless SQL for the
demo. Event Hubs Basic tier is enough for the vitals volume. Estimated demo cost
is a few dollars per day while the cluster is up; stop the cluster when not demoing.
