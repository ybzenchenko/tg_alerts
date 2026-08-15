-- Raw Athena external table for general_filters_symbol_processing_log_raw
-- Source files remain in S3; this statement only registers table metadata.
-- Raw CSV fields are intentionally defined as STRING to avoid ingestion issues
-- with blanks and mixed values. Cast to stronger types in downstream queries/models.
-- CSV columns that duplicate S3 partition names are renamed in Athena:
-- run_date -> csv_run_date, run_time -> csv_run_time

CREATE EXTERNAL TABLE IF NOT EXISTS funnel.general_filters_symbol_processing_log_raw (
    `run_datetime_utc` STRING,
    `csv_run_date` STRING,
    `csv_run_time` STRING,
    `symbol` STRING,
    `recent_activity_calc_status` STRING,
    `error_message` STRING,
    `elapsed_seconds` STRING,
    `avg_10m_range_pct_prev_12` STRING,
    `candles_used` STRING,
    `first_candle_time_utc` STRING,
    `last_candle_time_utc` STRING
)
PARTITIONED BY (
    `run_date` STRING,
    `run_time` STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"'
)
STORED AS TEXTFILE
LOCATION 's3://bin-tickers-yev/general-filters-logs/symbol_processing_log/'
TBLPROPERTIES (
    'skip.header.line.count' = '1'
);

-- Discover existing Hive-style partitions in S3.
MSCK REPAIR TABLE funnel.general_filters_symbol_processing_log_raw;

-- Optional validation:
-- SHOW PARTITIONS funnel.general_filters_symbol_processing_log_raw;
-- SELECT * FROM funnel.general_filters_symbol_processing_log_raw LIMIT 10;
