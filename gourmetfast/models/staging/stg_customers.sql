with source as (
    select *
    from {{ source('raw', 'raw_customers') }}
),

typed as (
    select
        customer_id,
        name as customer_name,
        email as customer_email,
        cast(signup_date as date) as signup_date
    from source
),

deduped as (
    select
        *
    from (
        select
            *,
            row_number() over (
                partition by customer_id
                order by signup_date desc
            ) as rn
        from typed
    )
    where rn = 1
)

select
    customer_id,
    customer_name,
    customer_email,
    signup_date
from deduped
