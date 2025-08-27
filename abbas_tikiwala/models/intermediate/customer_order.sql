{{ config(
    materialized='table',
    schema='intermediate',
    file_format='delta'
) }}

with customer_orders as (
    select
        customer_key,
        count(order_key) as total_orders,
        sum(ordertotal_price_amount) as total_order_amount,
        avg(total_price) as average_order_amount
    from
        {{ ref('customer') }} as customer
    inner join
        {{ ref('order') }} as orders
    on
        customer.customer_id = orders.customer_key
    group by
        customer_key
)

select
    customer_id,
    total_orders,
    total_order_amount,
    average_order_amount
from
    customer_orders;