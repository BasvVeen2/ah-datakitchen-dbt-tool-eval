{{ config(
    materialized='table',
    schema='intermediate',
    file_format='delta'
) }}

with customer_orders as (
    select
        customer_id,
        count(order_id) as total_orders,
        sum(order_amount) as total_order_amount,
        avg(order_amount) as average_order_amount
    from
        {{ ref('customer') }} as customer
    inner join
        {{ ref('order') }} as orders
    on
        customer.customer_id = orders.customer_id
    group by
        customer_id
)

select
    customer_id,
    total_orders,
    total_order_amount,
    average_order_amount
from
    customer_orders;