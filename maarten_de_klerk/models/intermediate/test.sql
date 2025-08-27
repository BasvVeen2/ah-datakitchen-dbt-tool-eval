{{ config(
    materialized='view',
    schema='intermediate',
    file_format='delta'
) }}

with cte_customer as (
    select * from {{ref("customer")}}
),
cte_order as (
    select * from {{ref("order")}}
)
select *
from cte_order 
inner join cte_customer 
    on cte_order.customer_key = cte_customer.customer_key