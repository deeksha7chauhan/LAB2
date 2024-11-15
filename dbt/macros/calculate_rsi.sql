{% macro calculate_rsi(column_name, period) %}
    AVG(CASE WHEN {{ column_name }} > LAG({{ column_name }}) OVER (PARTITION BY symbol ORDER BY date)
             THEN {{ column_name }} - LAG({{ column_name }}) OVER (PARTITION BY symbol ORDER BY date)
             ELSE 0 END) OVER (ORDER BY date ROWS BETWEEN {{ period - 1 }} PRECEDING AND CURRENT ROW) /
    NULLIF(
        AVG(ABS({{ column_name }} - LAG({{ column_name }}) OVER (PARTITION BY symbol ORDER BY date)))
        OVER (ORDER BY date ROWS BETWEEN {{ period - 1 }} PRECEDING AND CURRENT ROW),
        0
    ) * 100
{% endmacro %}