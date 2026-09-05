select
    -- Run metadata
    {{ parse_utc_timestamp('run_datetime_utc') }}
        as run_datetime_utc,

    try_cast(run_date as date) as run_date,

    {{ format_hhmm('run_time') }}
        as run_time,

    script_name,
    status,

    -- S3 paths
    general_filters_s3_key,
    run_summary_log_s3_key,
    symbol_processing_log_s3_key,

    -- Filter configuration
    quote_asset,

    try_cast(
        volume_threshold_usdt as double
    ) as volume_threshold_usdt,

    try_cast(
        range_6h_threshold_pct as double
    ) as range_6h_threshold_pct,

    try_cast(
        range_12h_threshold_pct as double
    ) as range_12h_threshold_pct,

    kline_source_interval,
    custom_candle_interval,

    try_cast(
        previous_candles_count as integer
    ) as previous_candles_count,

    try_cast(
        avg_10m_range_threshold_pct as double
    ) as avg_10m_range_threshold_pct,

    -- Symbol funnel counts
    try_cast(
        all_symbols_count as integer
    ) as all_symbols_count,

    try_cast(
        quote_asset_usdt_count as integer
    ) as quote_asset_usdt_count,

    try_cast(
        trading_status_count as integer
    ) as trading_status_count,

    try_cast(
        spot_trading_allowed_count as integer
    ) as spot_trading_allowed_count,

    try_cast(
        non_stable_fiat_count as integer
    ) as non_stable_fiat_count,

    try_cast(
        previous_version_logic_count as integer
    ) as previous_version_logic_count,

    try_cast(
        volume_24h_pass_count as integer
    ) as volume_24h_pass_count,

    -- Rolling statistics
    try_cast(
        rolling_stats_symbols_count as integer
    ) as rolling_stats_symbols_count,

    try_cast(
        rolling_6h_rows_loaded as integer
    ) as rolling_6h_rows_loaded,

    try_cast(
        rolling_6h_failed_batches as integer
    ) as rolling_6h_failed_batches,

    try_cast(
        rolling_6h_skipped_symbols as integer
    ) as rolling_6h_skipped_symbols,

    try_cast(
        rolling_12h_rows_loaded as integer
    ) as rolling_12h_rows_loaded,

    try_cast(
        rolling_12h_failed_batches as integer
    ) as rolling_12h_failed_batches,

    try_cast(
        rolling_12h_skipped_symbols as integer
    ) as rolling_12h_skipped_symbols,

    -- Movement filter
    try_cast(
        base_movement_filter_count as integer
    ) as base_movement_filter_count,

    -- Recent activity processing
    try_cast(
        recent_activity_symbols_processed as integer
    ) as recent_activity_symbols_processed,

    try_cast(
        recent_activity_stats_calculated_count as integer
    ) as recent_activity_stats_calculated_count,

    try_cast(
        recent_activity_not_enough_candles_count as integer
    ) as recent_activity_not_enough_candles_count,

    try_cast(
        recent_activity_error_count as integer
    ) as recent_activity_error_count,

    -- Final output
    try_cast(
        final_general_filters_count as integer
    ) as final_general_filters_count,

    try_cast(
        active_cycle_new_count as integer
    ) as active_cycle_new_count,

    try_cast(
        active_cycle_continued_count as integer
    ) as active_cycle_continued_count,

    previous_general_filters_s3_key,

    -- Runtime
    try_cast(
        elapsed_seconds as double
    ) as elapsed_seconds

from {{ source('funnel', 'general_filters_run_summary_raw') }}