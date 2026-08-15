-- Raw Athena external table for trigger_2_results_raw
-- Source files remain in S3; this statement only registers table metadata.
-- Raw CSV fields are intentionally defined as STRING to avoid ingestion issues
-- with blanks and mixed values. Cast to stronger types in downstream queries/models.
-- CSV columns that duplicate S3 partition names are renamed in Athena:
-- interval -> csv_interval, trigger_date -> csv_trigger_date, trigger_time -> csv_trigger_time

CREATE EXTERNAL TABLE IF NOT EXISTS funnel.trigger_2_results_raw (
    `trigger_id` STRING,
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
    `entry_price` STRING,
    `c1_open_time_utc` STRING,
    `c1_open` STRING,
    `c1_high` STRING,
    `c1_low` STRING,
    `c1_close` STRING,
    `c1_body_length` STRING,
    `c1_quote_volume` STRING,
    `c2_open_time_utc` STRING,
    `c2_open` STRING,
    `c2_high` STRING,
    `c2_low` STRING,
    `c2_close` STRING,
    `c2_body_length` STRING,
    `c2_quote_volume` STRING,
    `c3_open_time_utc` STRING,
    `c3_open` STRING,
    `c3_high` STRING,
    `c3_low` STRING,
    `c3_close` STRING,
    `c3_quote_volume` STRING,
    `c1_close_less_than_c2_close` STRING,
    `c2_close_less_than_c3_close` STRING,
    `close_lookback_candles_total_including_c3` STRING,
    `close_lookback_previous_candles_used` STRING,
    `highest_close_previous_39_candles` STRING,
    `c3_close_higher_than_previous_39_closes` STRING,
    `close_lookback_first_candle_open_time_utc` STRING,
    `close_lookback_last_candle_open_time_utc` STRING,
    `c3_body_length` STRING,
    `c3_upper_shadow_length` STRING,
    `c3_upper_shadow_to_body_ratio` STRING,
    `c3_upper_shadow_to_body_min_ratio` STRING,
    `c3_upper_shadow_pass` STRING,
    `c3_lower_shadow_length` STRING,
    `c3_lower_shadow_to_body_ratio` STRING,
    `c3_lower_shadow_to_body_max_ratio` STRING,
    `c3_lower_shadow_pass` STRING,
    `volume_confirmation_rule` STRING,
    `volume_lookback_candles_max` STRING,
    `volume_lookback_candles_used` STRING,
    `avg_quote_volume_previous_max_40_before_c1` STRING,
    `c1_quote_volume_vs_avg_previous_max_40_ratio` STRING,
    `c2_quote_volume_vs_avg_previous_max_40_ratio` STRING,
    `c1_volume_confirmation_pass` STRING,
    `c2_volume_confirmation_pass` STRING,
    `volume_lookback_first_candle_open_time_utc` STRING,
    `volume_lookback_last_candle_open_time_utc` STRING,
    `body_confirmation_rule` STRING,
    `body_lookback_candles_max` STRING,
    `body_lookback_candles_used` STRING,
    `avg_body_previous_max_40_before_c1` STRING,
    `c1_body_vs_avg_previous_max_40_ratio` STRING,
    `c2_body_vs_avg_previous_max_40_ratio` STRING,
    `c1_body_confirmation_pass` STRING,
    `c2_body_confirmation_pass` STRING,
    `body_lookback_first_candle_open_time_utc` STRING,
    `body_lookback_last_candle_open_time_utc` STRING,
    `trigger_candle_source_s3_key` STRING,
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
LOCATION 's3://bin-tickers-yev/trigger-2-results/'
TBLPROPERTIES (
    'skip.header.line.count' = '1'
);

-- Discover existing Hive-style partitions in S3.
MSCK REPAIR TABLE funnel.trigger_2_results_raw;

-- Optional validation:
-- SHOW PARTITIONS funnel.trigger_2_results_raw;
-- SELECT * FROM funnel.trigger_2_results_raw LIMIT 10;
