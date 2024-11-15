select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      -- tests/test_unique_date_symbol.sql
SELECT COUNT(*) = 0 AS test_passed
FROM (
    SELECT
        date,
        symbol,
        COUNT(*) AS record_count
    FROM stock.data.stock_data
    GROUP BY date, symbol
    HAVING COUNT(*) > 1
) duplicates
      
    ) dbt_internal_test