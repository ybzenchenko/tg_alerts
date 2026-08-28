SELECT
  trigger_date,
  symbol,
  interval
FROM {{ ref('stg_trigger_2_results') }}