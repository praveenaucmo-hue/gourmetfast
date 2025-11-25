with source as (
    select
        order_id,
        customer_id,
        product_id,
        cast(order_date as timestamp) as order_timestamp,
        cast(order_date as date) as order_date,
        cast(quantity as integer) as quantity,
        lower(trim(status)) as raw_status
    from {{ source('raw', 'raw_orders') }}
),

standardized_status as (
    select
        *,
        case
            when raw_status in ('pending', 'in_progress') then 'pending'
            when raw_status in ('shipped') then 'shipped'
            when raw_status in ('delivered', 'complete') then 'delivered'
            when raw_status in ('cancelled', 'canceled') then 'cancelled'
            else 'unknown'
        end as status
    from source
),

filtered as (
    select
        *
    from standardized_status
    where quantity > 0
),

deduped as (
    select
        *,
        row_number() over (
            partition by order_id, product_id
            order by order_timestamp desc
        ) as rn
    from filtered
)

select
    order_id,
    customer_id,
    product_id,
    order_timestamp,
    order_date,
    quantity,
    status
from deduped
where rn = 1
