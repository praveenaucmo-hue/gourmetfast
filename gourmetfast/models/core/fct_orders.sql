{{ config(
    materialized = 'incremental',
    unique_key = ['order_id', 'product_id']
) }}

with src as (
    select
        order_id,
        customer_id,
        product_id,
        order_timestamp,
        order_date,
        quantity,
        status
    from {{ ref('stg_orders') }}
    {% if is_incremental() %}
      where order_timestamp > (
          select coalesce(max(order_timestamp), '1900-01-01')
          from {{ this }}
      )
    {% endif %}
),

joined as (
    select
        o.order_id,
        o.customer_id,
        o.product_id,
        o.order_timestamp,
        o.order_date,
        o.quantity,
        o.status,
        c.customer_name,
        p.product_name,
        p.category,
        p.price,
        (o.quantity * p.price) as order_amount
    from src o
    left join {{ ref('dim_customers') }} c
        on o.customer_id = c.customer_id
    left join {{ ref('dim_products') }} p
        on o.product_id = p.product_id
)

select * from joined
