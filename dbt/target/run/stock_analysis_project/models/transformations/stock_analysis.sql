
  create or replace   view stock.data.stock_analysis
  
   as (
    -- models/transformations/stock_analysis.sql
WITH PriceChange AS (
    SELECT
        date,
        symbol,
        close,
        LAG(close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close
    FROM stock.data.stock_data
),
GainsLosses AS (
    SELECT
        date,
        symbol,
        close,
        CASE 
            WHEN close > prev_close THEN close - prev_close
            ELSE 0 
        END AS gain,
        CASE 
            WHEN close < prev_close THEN prev_close - close
            ELSE 0 
        END AS loss
    FROM PriceChange
)
SELECT
    date,
    symbol,
    close,
    -- 7-day moving average
    AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg_7d,
    -- RSI calculation
    CASE 
        WHEN AVG(loss) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) = 0 THEN 100
        ELSE 100 - (100 / (1 + (
            AVG(gain) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) /
            AVG(loss) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW)
        )))
    END AS rsi_14d
FROM GainsLosses
ORDER BY symbol, date
  );

