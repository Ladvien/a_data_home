{% macro generate_source(source_name, table_name) %}
    {{ source(source_name, table_name).include(database=False) }}
{% endmacro %}
