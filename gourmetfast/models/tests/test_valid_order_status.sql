select *
from {{ ref('stg_orders') }}
where status not in ('delivered', 'shipped', 'pending', 'returned')
