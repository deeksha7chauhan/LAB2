
  
    

        create or replace transient table stock.data.stock_data
         as
        (-- models/raw/stock_data.sql
SELECT *
FROM STOCK.data.market_dat
        );
      
  