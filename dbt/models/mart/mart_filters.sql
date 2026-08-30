{{ config(materialized='table') }}

SELECT
  p.run_date,
  r.percentage_of_continued,
  count(p.symbol) as symbols_cnt
FROM {{ ref('int_general_filters_symbol_processing_log') }} as p
JOIN {{ ref('int_general_filters_run_summary') }} as r on r.run_date = p.run_date
GROUP BY 1, 2
