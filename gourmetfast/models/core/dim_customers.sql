{{ config(materialized='incremental', unique_key='customer_id') }}

with src as (
    select
        customer_id,
        customer_name,
        email,
        signup_date
    from {{ ref('stg_customers') }}
    {% if is_incremental() %}
      where signup_date > (
          select coalesce(max(signup_date), '1900-01-01')
          from {{ this }}
      )
    {% endif %}
)

select
    customer_id,
    customer_name,
    email,
    signup_date
from src
