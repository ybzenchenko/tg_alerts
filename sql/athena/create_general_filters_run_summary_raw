-- Raw Athena external table for general_filters_run_summary_raw
-- Source files remain in S3; this statement only registers table metadata.
-- Raw CSV fields are intentionally defined as STRING to avoid ingestion issues
-- with blanks and mixed values. Cast to stronger types in downstream queries/models.
-- CSV columns that duplicate S3 partition names are renamed in Athena:
-- run_date -> csv_run_date, run_time -> csv_run_time

CREATE EXTERNAL TABLE IF NOT EXISTS funnel.general_filters_run_summary_raw (
    `run_datetime_utc` STRING,
    `csv_run_date` STRING,
    `csv_run_time` STRING,
    `script_name` STRING,
    `status` STRING,
    `general_filters_s3_key` STRING,
    `run_summary_log_s3_key` STRING,
    `symbol_processing_log_s3_key` STRING,
    `quote_asset` STRING,
    `volume_threshold_usdt` STRING,
    `range_6h_threshold_pct` STRING,
    `range_12h_threshold_pct` STRING,
    `kline_source_interval` STRING,
    `custom_candle_interval` STRING,
    `previous_candles_count` STRING,
    `avg_10m_range_threshold_pct` STRING,
    `all_symbols_count` STRING,
    `quote_asset_usdt_count` STRING,
    `trading_status_count` STRING,
    `spot_trading_allowed_count` STRING,
    `non_stable_fiat_count` STRING,
    `previous_version_logic_count` STRING,
    `volume_24h_pass_count` STRING,
    `rolling_stats_symbols_count` STRING,
    `rolling_6h_rows_loaded` STRING,
    `rolling_6h_failed_batches` STRING,
    `rolling_6h_skipped_symbols` STRING,
    `rolling_12h_rows_loaded` STRING,
    `rolling_12h_failed_batches` STRING,
    `rolling_12h_skipped_symbols` STRING,
    `base_movement_filter_count` STRING,
    `recent_activity_symbols_processed` STRING,
    `recent_activity_stats_calculated_count` STRING,
    `recent_activity_not_enough_candles_count` STRING,
    `recent_activity_error_count` STRING,
    `final_general_filters_count` STRING,
    `active_cycle_new_count` STRING,
    `active_cycle_continued_count` STRING,
    `previous_general_filters_s3_key` STRING,
    `elapsed_seconds` STRING
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
LOCATION 's3://bin-tickers-yev/general-filters-logs/run_summary/'
TBLPROPERTIES (
    'skip.header.line.count' = '1'
);

-- Discover existing Hive-style partitions in S3.
MSCK REPAIR TABLE funnel.general_filters_run_summary_raw;

-- Optional validation:
-- SHOW PARTITIONS funnel.general_filters_run_summary_raw;
-- SELECT * FROM funnel.general_filters_run_summary_raw LIMIT 10;
