select
    -- Identifiers
    trigger_id,

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

    try_cast(trigger_candle_date as date)
        as trigger_candle_date,

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

    try_cast(entry_date as date)
        as entry_date,

    {{ format_hhmm('entry_time') }}
        as entry_time,

    try_cast(entry_price as double)
        as entry_price,

    -- C1
    {{ parse_utc_timestamp('c1_open_time_utc') }}
        as c1_open_time_utc,

    try_cast(c1_open as double) as c1_open,
    try_cast(c1_high as double) as c1_high,
    try_cast(c1_low as double) as c1_low,
    try_cast(c1_close as double) as c1_close,
    try_cast(c1_body_length as double) as c1_body_length,
    try_cast(c1_quote_volume as double) as c1_quote_volume,

    -- C2
    {{ parse_utc_timestamp('c2_open_time_utc') }}
        as c2_open_time_utc,

    try_cast(c2_open as double) as c2_open,
    try_cast(c2_high as double) as c2_high,
    try_cast(c2_low as double) as c2_low,
    try_cast(c2_close as double) as c2_close,
    try_cast(c2_body_length as double) as c2_body_length,
    try_cast(c2_quote_volume as double) as c2_quote_volume,

    -- C3
    {{ parse_utc_timestamp('c3_open_time_utc') }}
        as c3_open_time_utc,

    try_cast(c3_open as double) as c3_open,
    try_cast(c3_high as double) as c3_high,
    try_cast(c3_low as double) as c3_low,
    try_cast(c3_close as double) as c3_close,
    try_cast(c3_quote_volume as double) as c3_quote_volume,

    -- Close-pattern checks
    try_cast(
        c1_close_less_than_c2_close as boolean
    ) as c1_close_less_than_c2_close,

    try_cast(
        c2_close_less_than_c3_close as boolean
    ) as c2_close_less_than_c3_close,

    try_cast(
        close_lookback_candles_total_including_c3 as integer
    ) as close_lookback_candles_total_including_c3,

    try_cast(
        close_lookback_previous_candles_used as integer
    ) as close_lookback_previous_candles_used,

    try_cast(
        highest_close_previous_39_candles as double
    ) as highest_close_previous_39_candles,

    try_cast(
        c3_close_higher_than_previous_39_closes as boolean
    ) as c3_close_higher_than_previous_39_closes,

    {{ parse_utc_timestamp('close_lookback_first_candle_open_time_utc') }}
        as close_lookback_first_candle_open_time_utc,

    {{ parse_utc_timestamp('close_lookback_last_candle_open_time_utc') }}
        as close_lookback_last_candle_open_time_utc,

    -- C3 rejection metrics
    try_cast(c3_body_length as double)
        as c3_body_length,

    try_cast(c3_upper_shadow_length as double)
        as c3_upper_shadow_length,

    try_cast(c3_upper_shadow_to_body_ratio as double)
        as c3_upper_shadow_to_body_ratio,

    try_cast(c3_upper_shadow_to_body_min_ratio as double)
        as c3_upper_shadow_to_body_min_ratio,

    try_cast(c3_upper_shadow_pass as boolean)
        as c3_upper_shadow_pass,

    try_cast(c3_lower_shadow_length as double)
        as c3_lower_shadow_length,

    try_cast(c3_lower_shadow_to_body_ratio as double)
        as c3_lower_shadow_to_body_ratio,

    try_cast(c3_lower_shadow_to_body_max_ratio as double)
        as c3_lower_shadow_to_body_max_ratio,

    try_cast(c3_lower_shadow_pass as boolean)
        as c3_lower_shadow_pass,

    -- Volume confirmation
    volume_confirmation_rule,

    try_cast(
        volume_lookback_candles_max as integer
    ) as volume_lookback_candles_max,

    try_cast(
        volume_lookback_candles_used as integer
    ) as volume_lookback_candles_used,

    try_cast(
        avg_quote_volume_previous_max_40_before_c1 as double
    ) as avg_quote_volume_previous_max_40_before_c1,

    try_cast(
        c1_quote_volume_vs_avg_previous_max_40_ratio as double
    ) as c1_quote_volume_vs_avg_previous_max_40_ratio,

    try_cast(
        c2_quote_volume_vs_avg_previous_max_40_ratio as double
    ) as c2_quote_volume_vs_avg_previous_max_40_ratio,

    try_cast(
        c1_volume_confirmation_pass as boolean
    ) as c1_volume_confirmation_pass,

    try_cast(
        c2_volume_confirmation_pass as boolean
    ) as c2_volume_confirmation_pass,

    {{ parse_utc_timestamp('volume_lookback_first_candle_open_time_utc') }}
        as volume_lookback_first_candle_open_time_utc,

    {{ parse_utc_timestamp('volume_lookback_last_candle_open_time_utc') }}
        as volume_lookback_last_candle_open_time_utc,

    -- Body confirmation
    body_confirmation_rule,

    try_cast(
        body_lookback_candles_max as integer
    ) as body_lookback_candles_max,

    try_cast(
        body_lookback_candles_used as integer
    ) as body_lookback_candles_used,

    try_cast(
        avg_body_previous_max_40_before_c1 as double
    ) as avg_body_previous_max_40_before_c1,

    try_cast(
        c1_body_vs_avg_previous_max_40_ratio as double
    ) as c1_body_vs_avg_previous_max_40_ratio,

    try_cast(
        c2_body_vs_avg_previous_max_40_ratio as double
    ) as c2_body_vs_avg_previous_max_40_ratio,

    try_cast(
        c1_body_confirmation_pass as boolean
    ) as c1_body_confirmation_pass,

    try_cast(
        c2_body_confirmation_pass as boolean
    ) as c2_body_confirmation_pass,

    {{ parse_utc_timestamp('body_lookback_first_candle_open_time_utc') }}
        as body_lookback_first_candle_open_time_utc,

    {{ parse_utc_timestamp('body_lookback_last_candle_open_time_utc') }}
        as body_lookback_last_candle_open_time_utc,

    -- S3 lineage
    {{ null_if_blank('trigger_candle_source_s3_key') }}
        as trigger_candle_source_s3_key,

    {{ null_if_blank('recent_activity_s3_key') }}
        as recent_activity_s3_key,

    -- Processing metadata
    {{ parse_utc_timestamp('trigger_checker_run_datetime_utc') }}
        as trigger_checker_run_datetime_utc,

    trigger_rule

from {{ source('funnel', 'trigger_2_results_raw') }}