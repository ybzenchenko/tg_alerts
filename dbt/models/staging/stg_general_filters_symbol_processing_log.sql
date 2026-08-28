{{ config(materialized='view') }}

select

    -- Run metadata
    try(
        date_parse(
            substr(run_datetime_utc, 1, 19),
            '%Y-%m-%d %H:%i:%s'
        )
    ) as run_datetime_utc,

    try_cast(run_date as date) as run_date,

    concat(
        substr(lpad(run_time, 4, '0'), 1, 2),
        ':',
        substr(lpad(run_time, 4, '0'), 3, 2)
    ) as run_time,

    -- Symbol
    symbol,

    -- Processing result
    recent_activity_calc_status,

    nullif(trim(error_message), '') as error_message,

    -- Metrics
    try_cast(elapsed_seconds as double) as elapsed_seconds,

    try_cast(
        avg_10m_range_pct_prev_12 as double
    ) as avg_10m_range_pct_prev_12,

    try_cast(
        candles_used as integer
    ) as candles_used,

    -- Candle window
    try(
        date_parse(
            substr(first_candle_time_utc, 1, 19),
            '%Y-%m-%d %H:%i:%s'
        )
    ) as first_candle_time_utc,

    try(
        date_parse(
            substr(last_candle_time_utc, 1, 19),
            '%Y-%m-%d %H:%i:%s'
        )
    ) as last_candle_time_utc

from {{ source('funnel', 'general_filters_symbol_processing_log_raw') }}