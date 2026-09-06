# TG Alerts — dbt Analytics

This folder contains the dbt analytics layer for the **TG Alerts** crypto alert pipeline.

The dbt project transforms raw operational data stored in Amazon S3 and exposed through AWS Glue/Athena into typed staging models, reusable intermediate models, and analytics-ready mart tables.

## Architecture

```text
Binance API / WebSocket
        ↓
Python alert and filter services
        ↓
Amazon S3
        ↓
AWS Glue Data Catalog
        ↓
Amazon Athena
        ↓
dbt
  ├── staging
  ├── intermediate
  └── marts
```

dbt does not ingest the raw files directly. Raw CSV data is stored in S3 and registered as external Athena tables. dbt reads those tables through Athena and creates the transformation layer.

---

## Project structure

```text
dbt/
├── dbt_project.yml
├── README.md
│
├── models/
│   ├── staging/
│   │   ├── sources.yml
│   │   ├── stg_general_filters_run_summary.sql
│   │   ├── stg_general_filters_symbol_processing_log.sql
│   │   ├── stg_trigger_1_results.sql
│   │   └── stg_trigger_2_results.sql
│   │
│   ├── int/
│   │   ├── int_general_filters_run_summary.sql
│   │   ├── int_general_filters_symbol_processing_log.sql
│   │   ├── int_trigger_1_results.sql
│   │   └── int_trigger_2_results.sql
│   │
│   └── mart/
│       ├── mart_filters.sql
│       ├── mart_filters.yml
│       ├── mart_symbols.sql
│       └── mart_symbols.yml
│
├── macros/
│   ├── parse_utc_timestamp.sql
│   ├── format_hhmm.sql
│   └── null_if_blank.sql
│
├── tests/
├── analyses/
├── seeds/
└── snapshots/
```

---

## Data sources

The project currently uses four Athena source tables in the `funnel` schema:

- `trigger_1_results_raw`
- `trigger_2_results_raw`
- `general_filters_run_summary_raw`
- `general_filters_symbol_processing_log_raw`

They are declared in:

```text
models/staging/sources.yml
```

The source tables are backed by partitioned CSV data in Amazon S3.

---

## Transformation layers

### Staging

The staging layer cleans and standardizes raw Athena data.

Main responsibilities:

- cast raw string fields to the correct types
- parse UTC timestamps
- convert partition time values from `HHMM` to `HH:MM`
- convert empty strings to `NULL` where appropriate
- use S3 partition fields as canonical date/time metadata
- preserve S3 lineage and trigger metadata

Staging models are materialized as **views**.

### Intermediate

The intermediate layer contains reusable transformations and business logic built on top of staging models.

Intermediate models are materialized as **views**.

### Marts

The mart layer provides analytics-ready datasets for reporting and investigation.

Current marts:

- `mart_filters`
- `mart_symbols`

Mart models are materialized as **tables**.

Materialization settings are defined centrally in `dbt_project.yml`.

---

## Macros

Repeated transformation logic is moved into reusable dbt macros.

### `parse_utc_timestamp`

Parses UTC timestamp strings into Athena-compatible timestamps.

Example:

```sql
{{ parse_utc_timestamp('trigger_candle_open_time_utc') }}
```

### `format_hhmm`

Converts partition-style time strings such as `0105` into `01:05`.

Example:

```sql
{{ format_hhmm('trigger_time') }}
```

### `null_if_blank`

Converts empty or whitespace-only strings to `NULL`.

Example:

```sql
{{ null_if_blank('error_message') }}
```

Macros keep staging SQL consistent and reduce duplicated parsing logic.

---

## Tests

The project uses both built-in dbt data tests and custom singular SQL tests.

Examples of built-in tests:

```yaml
data_tests:
  - not_null
  - unique
```

Current mart-level expectations include:

- `mart_filters.run_date` is not null and unique
- `mart_symbols.symbol` is not null and unique

Source-level tests validate important raw fields such as trigger IDs and timestamps.

Custom singular tests can be stored in:

```text
tests/
```

A singular test passes when its SQL query returns **zero rows**.

Example:

```sql
select
    symbol,
    count(*) as row_count
from {{ ref('mart_symbols') }}
group by symbol
having count(*) > 1
```

The project currently configures data tests to return warnings by default:

```yaml
data_tests:
  +severity: warn
```

Individual critical tests can override this with `severity: error`.

---

## Source freshness

Freshness checks are configured for the general-filter log sources using `run_datetime_utc`.

Run:

```bash
dbt source freshness
```

> Note: if the TG Alerts production collection is intentionally stopped, freshness warnings are expected because no new source data is arriving.

---

## Local environment

The project is developed locally with a dedicated Conda environment:

```text
dbt-athena
```

Current working versions:

```text
dbt Core:   1.12.x
dbt Athena: 1.11.x
```

Activate the environment in Windows / VS Code:

```bat
call C:\ProgramData\anaconda3\Scripts\activate.bat C:\ProgramData\anaconda3\envs\dbt-athena
```

Then move into the dbt project:

```bat
cd C:\Users\Admin\Desktop\git\tg_alerts\dbt
```

Verify:

```bat
dbt --version
```

---

## Athena connection

The local dbt profile is stored outside the repository:

```text
C:\Users\Admin\.dbt\profiles.yml
```

Important configuration:

```text
adapter:        Athena
catalog:        awsdatacatalog
schema:         tg_alerts_analytics
region:         eu-north-1

S3 query results:
s3://bin-tickers-yev/athena-query-results/dbt/

dbt-managed data:
s3://bin-tickers-yev/analytics/dbt/
```

`profiles.yml` should **not** be committed because connection configuration belongs outside the project repository.

AWS authentication is resolved through the normal AWS/boto3 credential chain.

---

## Common dbt commands

Run all models:

```bash
dbt run
```

Run only staging models:

```bash
dbt run --select path:models/staging
```

Run only marts:

```bash
dbt run --select path:models/mart
```

Run all tests:

```bash
dbt test
```

Test a specific model:

```bash
dbt test --select mart_symbols
```

Run source tests:

```bash
dbt test --select source:funnel
```

Check source freshness:

```bash
dbt source freshness
```

Compile Jinja/macros without executing models:

```bash
dbt compile
```

Build models and associated tests according to the dbt DAG:

```bash
dbt build
```

---

## Documentation and lineage

Generate dbt documentation:

```bash
dbt docs generate
```

Serve it locally:

```bash
dbt docs serve
```

The generated documentation provides:

- model and source documentation
- column metadata
- tests
- dependencies
- model lineage / DAG

Generated artifacts are written to:

```text
target/
```

`target/` is build output and should not be committed to Git.

---

## Git

Recommended dbt-specific ignored files:

```gitignore
target/
dbt_packages/
logs/
.env
```

Source-controlled dbt content should include:

```text
dbt_project.yml
models/
macros/
tests/
analyses/
seeds/
snapshots/
```

---

## Planned improvements

Useful next steps for the dbt layer include:

- additional business-rule singular tests
- unit tests for transformation logic
- model and column descriptions
- `dbt-utils` package
- one incremental model for append-only historical data
- Airflow orchestration for dbt execution and testing
- downstream exposures once dashboards or other consumers are added

Seeds and snapshots will only be introduced if there is a real project requirement rather than adding them artificially.

---

## Role in TG Alerts

The dbt project separates analytics transformation logic from the Python production pipeline.

Python is responsible for collecting and producing operational data. S3 stores the raw history. Athena and Glue make the data queryable. dbt then provides a tested, documented, version-controlled transformation layer suitable for analytics and future reporting.
