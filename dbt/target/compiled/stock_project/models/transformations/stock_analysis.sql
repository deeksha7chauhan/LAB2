-- models/transformations/stock_analysis.sql
SELECT
    date,
    symbol,
    close,
    AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg_7d,
    SUM(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS rsi_14d
FROM stock.data.stock_data