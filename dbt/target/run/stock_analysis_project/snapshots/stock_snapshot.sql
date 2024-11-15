
      
  
    

        create or replace transient table STOCK.snapshots.stock_snapshot
         as
        (
    

    select *,
        md5(coalesce(cast(date_symbol as varchar ), '')
         || '|' || coalesce(cast(updated_at as varchar ), '')
        ) as dbt_scd_id,
        updated_at as dbt_updated_at,
        updated_at as dbt_valid_from,
        
  
  coalesce(nullif(updated_at, updated_at), null)
  as dbt_valid_to

    from (
        
    
    
    SELECT 
        date,
        symbol,
        open,
        high,
        low,
        close,
        volume,
        CONCAT(date, '_', symbol) AS date_symbol,
        CURRENT_TIMESTAMP AS updated_at
    FROM stock.data.stock_data
    ) sbq



        );
      
  
  