select *
from {{ ref('stg_trigger_2_results') }}
where not (
   c1_close < c2_close
   and c2_close < c3_close
   and c3_close > highest_close_previous_39_candles
   and c3_upper_shadow_pass = true
   and c3_lower_shadow_pass = true
   and c1_volume_confirmation_pass = true
   and c2_volume_confirmation_pass = true
   and c1_body_confirmation_pass = true
   and c2_body_confirmation_pass = true
)