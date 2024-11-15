
  
    

        create or replace transient table stock.data.stock_mart
         as
        (-- models/marts/stock_mart.sql
SELECT
    date,
    symbol,
    close,
    moving_avg_7d,
    rsi_14d
FROM stock.data.stock_analysis
WHERE date >= CURRENT_DATE - INTERVAL '90 DAY'
        );
      
  