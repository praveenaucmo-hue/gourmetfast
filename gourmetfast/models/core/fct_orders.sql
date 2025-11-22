{{ config(materialized='incremental', unique_key='order_id') }}

with orders as (
    select *
    from {{ ref('stg_orders') }}

    {% if is_incremental() %}
        where order_date > (select max(order_date) from {{ this }})
    {% endif %}
),

joined as (
    select
        o.order_id,
        o.customer_id,
        o.product_id,
        o.order_date,
        o.quantity,
        o.status,
        p.price,
        (o.quantity * p.price) as revenue
    from orders o
    left join {{ ref('dim_products') }} p
        on o.product_id = p.product_id
)

select *
from joined
