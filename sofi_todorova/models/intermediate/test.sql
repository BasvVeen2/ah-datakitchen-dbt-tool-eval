{{ config(
    materialized='view',
    schema='intermediate'
) }}
select
    o.order_key,
    o.order_status,
    o.order_date,
    o.order_priority,
    o.total_price,
    c.customer_key,
    c.phone,
    c.account_balance
from {{ref('order')}} as o
inner join {{ref('customer')}} as c on
    o.customer_key = c.customer_key
where o.order_priority in ('2-HIGH', '1-URGENT')
