select *
from {{ ref('stg_trigger_1_results') }}
where trigger_candle_close >= swing_low_price or volume_confirmation_pass <> true or trigger_quote_volume_vs_avg_previous_max_40_ratio <= 1