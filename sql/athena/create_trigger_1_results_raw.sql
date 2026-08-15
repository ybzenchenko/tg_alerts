-- Raw Athena external table for trigger_1_results_raw
-- Source files remain in S3; this statement only registers table metadata.
-- Raw CSV fields are intentionally defined as STRING to avoid ingestion issues
-- with blanks and mixed values. Cast to stronger types in downstream queries/models.
-- CSV columns that duplicate S3 partition names are renamed in Athena:
-- interval -> csv_interval, trigger_date -> csv_trigger_date, trigger_time -> csv_trigger_time

CREATE EXTERNAL TABLE IF NOT EXISTS funnel.trigger_1_results_raw (
    `trigger_id` STRING,
    `swing_low_id` STRING,
    `csv_interval` STRING,
    `trigger_type` STRING,
    `symbol` STRING,
    `csv_trigger_date` STRING,
    `csv_trigger_time` STRING,
    `trigger_discovered_datetime_utc` STRING,
    `trigger_candle_date` STRING,
    `trigger_candle_time` STRING,
    `trigger_candle_open_time_utc` STRING,
    `trigger_candle_close_time_utc` STRING,
    `trigger_candle_event_close_time_utc` STRING,
    `entry_time_utc` STRING,
    `entry_date` STRING,
    `entry_time` STRING,
    `swing_low_open_time_utc` STRING,
    `swing_low_price` STRING,
    `confirmation_candle_open_time_utc` STRING,
    `trigger_candle_open` STRING,
    `trigger_candle_high` STRING,
    `trigger_candle_low` STRING,
    `trigger_candle_close` STRING,
    `entry_price` STRING,
    `trigger_candle_volume` STRING,
    `trigger_candle_quote_volume` STRING,
    `trigger_candle_number_of_trades` STRING,
    `volume_confirmation_required` STRING,
    `volume_confirmation_rule` STRING,
    `volume_confirmation_pass` STRING,
    `volume_lookback_candles_max` STRING,
    `volume_lookback_candles_used` STRING,
    `avg_quote_volume_previous_max_40_candles` STRING,
    `trigger_quote_volume_vs_avg_previous_max_40_ratio` STRING,
    `volume_lookback_first_candle_open_time_utc` STRING,
    `volume_lookback_last_candle_open_time_utc` STRING,
    `trigger_candle_source_s3_key` STRING,
    `swing_low_source_s3_key` STRING,
    `recent_activity_s3_key` STRING,
    `trigger_checker_run_datetime_utc` STRING,
    `trigger_rule` STRING
)
PARTITIONED BY (
    `interval` STRING,
    `trigger_date` STRING,
    `trigger_time` STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"'
)
STORED AS TEXTFILE
LOCATION 's3://bin-tickers-yev/trigger-1-results/'
TBLPROPERTIES (
    'skip.header.line.count' = '1'
);

-- Discover existing Hive-style partitions in S3.
MSCK REPAIR TABLE funnel.trigger_1_results_raw;

-- Optional validation:
-- SHOW PARTITIONS funnel.trigger_1_results_raw;
-- SELECT * FROM funnel.trigger_1_results_raw LIMIT 10;
