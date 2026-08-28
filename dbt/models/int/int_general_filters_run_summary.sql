SELECT
  run_date,
  final_general_filters_count,
  active_cycle_new_count,
  active_cycle_continued_count,
  round(cast(active_cycle_continued_count as double) / cast(final_general_filters_count as double) * 100, 1) as percentage_of_continued
FROM {{ ref('stg_general_filters_run_summary') }}
