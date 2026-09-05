SELECT
  symbol,
  count(*) as total_cnt
FROM {{ ref('int_general_filters_symbol_processing_log') }}
GROUP BY 1