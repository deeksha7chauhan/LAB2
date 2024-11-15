-- models/raw/stock_data.sql

WITH stock_data_cte AS (
    SELECT *
    FROM STOCK.data.market_dat
)

SELECT *
FROM stock_data_cte