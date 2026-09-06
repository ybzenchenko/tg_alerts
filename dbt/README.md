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
│   ├── assert_general_filter_counts_reconcile.sql
│   ├── assert_percentage_of_continued_valid.sql
│   ├── assert_trigger_1_rules.sql
│   └── assert_trigger_2_rules.sql
│
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

### Custom singular tests

Custom tests are stored in:

```text
tests/
```

A singular dbt test passes when its SQL query returns **zero rows**. Each test therefore selects records that violate an expected business rule or data-quality condition.

#### `assert_general_filter_counts_reconcile.sql`

Checks that the final number of symbols in a general-filter run reconciles with the two active-cycle groups:

```text
final_general_filters_count
=
active_cycle_new_count
+
active_cycle_continued_count
```

The test returns rows where this relationship does not hold.

#### `assert_percentage_of_continued_valid.sql`

Validates that `percentage_of_continued` remains within the expected range:

```text
0 to 100
```

Any record below `0` or above `100` is returned as a test failure.

#### `assert_trigger_1_rules.sql`

Independently validates important Trigger 1 conditions against the stored trigger results.

The test identifies records where any of the following is true:

- the trigger candle close is not below the swing-low price
- volume confirmation did not pass
- trigger quote volume is not greater than the previous lookback average

This provides a dbt-side validation of the Trigger 1 logic produced by the Python alert engine.

#### `assert_trigger_2_rules.sql`

Validates the main conditions required for a Trigger 2 result, including:

- `C1 close < C2 close`
- `C2 close < C3 close`
- C3 closes above the highest close of the previous 39 candles
- C3 upper-shadow condition passes
- C3 lower-shadow condition passes
- C1 and C2 volume confirmations pass
- C1 and C2 body confirmations pass

Rows are returned when the stored Trigger 2 result does not satisfy all of these conditions.

These custom tests complement generic `not_null` and `unique` checks by validating business logic and the consistency of the alert-generation pipeline.

The project currently configures data tests to return warnings by default:

```yaml
data_tests:
  +severity: warn
```

Individual critical tests can override this with `severity: error`.

---

## Role in TG Alerts

The dbt project separates analytics transformation logic from the Python production pipeline.

Python is responsible for collecting and producing operational data. S3 stores the raw history. Athena and Glue make the data queryable. dbt then provides a tested, documented, version-controlled transformation layer suitable for analytics and future reporting.
