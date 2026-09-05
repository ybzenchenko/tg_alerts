select
    -- Identifiers
    trigger_id,
    swing_low_id,

    -- Use S3 partition values as canonical fields
    interval,

    trigger_type,
    symbol,

    try_cast(trigger_date as date) as trigger_date,

    {{ format_hhmm('trigger_time') }}
        as trigger_time,

    -- Trigger timestamps
    {{ parse_utc_timestamp('trigger_discovered_datetime_utc') }}
        as trigger_discovered_datetime_utc,

    try_cast(trigger_candle_date as date) as trigger_candle_date,

    {{ format_hhmm('trigger_candle_time') }}
        as trigger_candle_time,

    {{ parse_utc_timestamp('trigger_candle_open_time_utc') }}
        as trigger_candle_open_time_utc,

    {{ parse_utc_timestamp('trigger_candle_close_time_utc') }}
        as trigger_candle_close_time_utc,

    {{ parse_utc_timestamp('trigger_candle_event_close_time_utc') }}
        as trigger_candle_event_close_time_utc,

    -- Entry
    {{ parse_utc_timestamp('entry_time_utc') }}
        as entry_time_utc,

    try_cast(entry_date as date) as entry_date,

    {{ format_hhmm('entry_time') }}
        as entry_time,

    -- Swing low
    {{ parse_utc_timestamp('swing_low_open_time_utc') }}
        as swing_low_open_time_utc,

    try_cast(
        swing_low_price as double
    ) as swing_low_price,

    {{ parse_utc_timestamp('confirmation_candle_open_time_utc') }}
        as confirmation_candle_open_time_utc,

    -- Trigger candle OHLC
    try_cast(trigger_candle_open as double)
        as trigger_candle_open,

    try_cast(trigger_candle_high as double)
        as trigger_candle_high,

    try_cast(trigger_candle_low as double)
        as trigger_candle_low,

    try_cast(trigger_candle_close as double)
        as trigger_candle_close,

    try_cast(entry_price as double)
        as entry_price,

    -- Candle activity
    try_cast(trigger_candle_volume as double)
        as trigger_candle_volume,

    try_cast(
        trigger_candle_quote_volume as double
    ) as trigger_candle_quote_volume,

    try_cast(
        trigger_candle_number_of_trades as integer
    ) as trigger_candle_number_of_trades,

    -- Volume confirmation
    try_cast(
        volume_confirmation_required as boolean
    ) as volume_confirmation_required,

    volume_confirmation_rule,

    try_cast(
        volume_confirmation_pass as boolean
    ) as volume_confirmation_pass,

    try_cast(
        volume_lookback_candles_max as integer
    ) as volume_lookback_candles_max,

    try_cast(
        volume_lookback_candles_used as integer
    ) as volume_lookback_candles_used,

    try_cast(
        avg_quote_volume_previous_max_40_candles as double
    ) as avg_quote_volume_previous_max_40_candles,

    try_cast(
        trigger_quote_volume_vs_avg_previous_max_40_ratio as double
    ) as trigger_quote_volume_vs_avg_previous_max_40_ratio,

    {{ parse_utc_timestamp('volume_lookback_first_candle_open_time_utc') }}
        as volume_lookback_first_candle_open_time_utc,

    {{ parse_utc_timestamp('volume_lookback_last_candle_open_time_utc') }}
        as volume_lookback_last_candle_open_time_utc,

    -- S3 lineage
    {{ null_if_blank('trigger_candle_source_s3_key') }}
        as trigger_candle_source_s3_key,

    {{ null_if_blank('swing_low_source_s3_key') }}
        as swing_low_source_s3_key,

    {{ null_if_blank('recent_activity_s3_key') }}
        as recent_activity_s3_key,

    -- Trigger processing metadata
    {{ parse_utc_timestamp('trigger_checker_run_datetime_utc') }}
        as trigger_checker_run_datetime_utc,

    trigger_rule

from {{ source('funnel', 'trigger_1_results_raw') }}