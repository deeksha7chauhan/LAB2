
    
    

select
    symbol as unique_field,
    count(*) as n_records

from stock.data.stock_data
where symbol is not null
group by symbol
having count(*) > 1


