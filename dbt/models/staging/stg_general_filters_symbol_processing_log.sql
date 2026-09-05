select
    -- Run metadata
    {{ parse_utc_timestamp('run_datetime_utc') }}
        as run_datetime_utc,

    try_cast(run_date as date) as run_date,

    {{ format_hhmm('run_time') }}
        as run_time,

    -- Symbol
    symbol,

    -- Processing result
    recent_activity_calc_status,

    {{ null_if_blank('error_message') }}
        as error_message,

    -- Metrics
    try_cast(
        elapsed_seconds as double
    ) as elapsed_seconds,

    try_cast(
        avg_10m_range_pct_prev_12 as double
    ) as avg_10m_range_pct_prev_12,

    try_cast(
        candles_used as integer
    ) as candles_used,

    -- Candle window
    {{ parse_utc_timestamp('first_candle_time_utc') }}
        as first_candle_time_utc,

    {{ parse_utc_timestamp('last_candle_time_utc') }}
        as last_candle_time_utc

from {{ source('funnel', 'general_filters_symbol_processing_log_raw') }}