{{ config(
    materialized = 'view'
) }}
    
with source as (
    select
        product_id,
        name as product_name,
        category,
        cast(price as numeric(18, 2)) as price
    from {{ source('raw', 'raw_products') }}
),

cleaned as (
    select
        product_id,
        product_name,
        nullif(category, '') as category,
        case
            when price < 0 then null
            else price
        end as price
    from source
),

deduped as (
    select
        product_id,
        product_name,
        category,
        price,
        row_number() over (
            partition by product_id
            order by product_name desc
        ) as rn
    from cleaned
)

select
    product_id,
    product_name,
    category,
    price
from deduped
where rn = 1
