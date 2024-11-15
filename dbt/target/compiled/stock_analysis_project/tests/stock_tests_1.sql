-- tests/test_no_nulls_in_date.sql
SELECT COUNT(*) = 0 AS test_passed
FROM stock.data.stock_data
WHERE date IS NULL