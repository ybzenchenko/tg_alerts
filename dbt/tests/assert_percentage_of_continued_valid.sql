select
    *
from {{ ref('int_general_filters_run_summary') }}
where percentage_of_continued < 0 or percentage_of_continued > 100