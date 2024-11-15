-- snapshots/stock_snapshot.sql
{% snapshot stock_snapshot %}
    {{
        config(
            target_schema='snapshots',
            target_database='STOCK',
            unique_key='date_symbol',
            strategy='timestamp',
            updated_at='updated_at'
        )
    }}
    
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
    FROM {{ ref('stock_data') }}
{% endsnapshot %}