{{ config(materialized='incremental', unique_key='customer_id') }}

with src as (

    select
        customer_id,
        customer_name,
        customer_email,
        signup_date
    from {{ ref('stg_customers') }}

    {% if is_incremental() and execute %}
      where datetime(signup_date) > (
          select datetime(
              coalesce(max_ts, '1900-01-01')
          )
          from (
              select max(signup_date) as max_ts
              from {{ this }}
          )
      )
    {% endif %}

)

select
    customer_id,
    customer_name,
    customer_email,
    signup_date
from src;
