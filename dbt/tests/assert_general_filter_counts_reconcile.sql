select
    run_date,
    final_general_filters_count,
    active_cycle_new_count,
    active_cycle_continued_count
from {{ ref('int_general_filters_run_summary') }}
where final_general_filters_count <> active_cycle_new_count + active_cycle_continued_count