-- models/marts/stock_mart.sql

WITH stock_analysis_cte AS (
    SELECT
        date,
        symbol,
        close,
        moving_avg_7d,
        rsi_14d
    FROM {{ ref('stock_analysis') }}
),

filtered_stock_data AS (
    SELECT *
    FROM stock_analysis_cte
    WHERE date >= CURRENT_DATE - INTERVAL '90 DAY'
)

SELECT
    date,
    symbol,
    close,
    moving_avg_7d,
    rsi_14d
FROM filtered_stock_data