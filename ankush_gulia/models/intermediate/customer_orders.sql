{{ config(
    materialized='view',
    schema='intermediate',
    file_format='delta'
) }}

with customer_data as (
    select
        customer_key,
        customer_name,
        nation_key,
        phone,
        account_balance,
        market_segment,
        comment,
        address,
        contact_methods,
        account_history
    from {{ ref('customer') }}
),
order_data as (
    select
        order_key,
        customer_key,
        order_status,
        total_price,
        order_date,
        order_priority,
        clerk,
        ship_priority,
        comment
    from {{ ref('order') }}
)

select
    customer_data.customer_key,
    customer_data.customer_name,
    order_data.order_key,
    order_data.total_price,
    order_data.order_date
from customer_data
inner join order_data on customer_data.customer_key = order_data.customer_key