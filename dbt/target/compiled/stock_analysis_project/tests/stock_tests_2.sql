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