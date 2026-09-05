{% macro parse_utc_timestamp(column_name) %}

try(
    date_parse(
        replace(
            substr(trim({{ column_name }}), 1, 19),
            'T',
            ' '
        ),
        '%Y-%m-%d %H:%i:%s'
    )
)

{% endmacro %}