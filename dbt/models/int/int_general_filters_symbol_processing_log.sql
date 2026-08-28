SELECT
  run_date,
  symbol
FROM {{ ref('stg_general_filters_symbol_processing_log') }}