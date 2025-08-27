{{ config(
    materialized='table',
    schema='intermediate',
    file_format='delta'
) }}

with customer_orders as (
    select
        customer.customer_key as customer_key,
        count(order_key) as total_orders,
        sum(total_price) as total_order_amount,
        avg(total_price) as average_order_amount
    from
        {{ ref('customer') }} as customer
    inner join
        {{ ref('order') }} as orders
    on
        customer.customer_key = orders.customer_key
    group by
        customer.customer_key
)

select
    customer_key,
    total_orders,
    total_order_amount,
    average_order_amount
from
    customer_orders;