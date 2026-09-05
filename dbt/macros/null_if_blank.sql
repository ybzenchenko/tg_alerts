{% macro null_if_blank(column_name) %}

nullif(trim({{ column_name }}), '')

{% endmacro %}