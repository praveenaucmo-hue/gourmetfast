{{ config(materialized='incremental', unique_key='customer_id') }}

select *
from {{ ref('stg_customers') }}
