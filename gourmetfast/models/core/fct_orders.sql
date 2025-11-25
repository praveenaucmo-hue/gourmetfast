{{ config(
    materialized = 'incremental',
    unique_key = 'order_id'
) }}

with src as (

    select *
    from {{ ref('stg_orders') }}

    {% if is_incremental() and execute %}
      where datetime(order_timestamp) > (
          select datetime(
              coalesce(max_ts, '1900-01-01')
          )
          from (
              select max(order_timestamp) as max_ts
              from {{ this }}
          )
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

select * from joined;
