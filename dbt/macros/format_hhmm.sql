{% macro format_hhmm(column_name) %}

concat(
    substr(lpad(trim({{ column_name }}), 4, '0'), 1, 2),
    ':',
    substr(lpad(trim({{ column_name }}), 4, '0'), 3, 2)
)

{% endmacro %}