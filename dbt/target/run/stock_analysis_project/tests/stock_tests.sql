select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      -- tests/stock_tests.sql

-- Test to ensure there are no null values in the 'date' column
WITH invalid_date AS (
    SELECT *
    FROM stock.data.stock_data
    WHERE date IS NULL
)
SELECT COUNT(*) = 0 AS test_passed
FROM invalid_date

-- Test to ensure each date-symbol combination is unique
WITH duplicate_check AS (
    SELECT
        date,
        symbol,
        COUNT(*) AS record_count
    FROM stock.data.stock_data
    GROUP BY date, symbol
    HAVING COUNT(*) > 1
)
SELECT COUNT(*) = 0 AS test_passed
FROM duplicate_check
      
    ) dbt_internal_test