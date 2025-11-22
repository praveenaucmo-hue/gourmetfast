with source as (
    select *
    from {{ source('raw', 'raw_orders') }}
),

typed as (
    select
        order_id,
        customer_id,
        product_id,
        cast(order_date as date) as order_date,
        cast(quantity as int) as quantity,
        lower(status) as status
    from source
),

cleaned as (
    select *
    from typed
    where quantity > 0
      and status in ('delivered', 'shipped', 'pending', 'returned')
)

select *
from cleaned
