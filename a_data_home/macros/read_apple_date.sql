{% macro read_apple_date(col) %}
    (TIMESTAMP '2001-01-01' + INTERVAL ({{ col }} / 1000000000.0) SECOND)   -- noqa:
{% endmacro %}
