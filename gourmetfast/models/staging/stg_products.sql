with source as (
    select *
    from {{ source('raw', 'raw_products') }}
)

select
    product_id,
    name as product_name,
    category,
    cast(price as float) as price
from source
