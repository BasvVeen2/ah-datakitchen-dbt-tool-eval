{{ config(
    materialized="table",
    tags=["mytag"]
)
 }}

with
    ranked_orders as (
        select
            c.customer_key,
            c.customer_name,
            c.account_balance as customer_account_balance,
            o.order_date,
            o.total_price,
            row_number() over (
                partition by c.customer_key order by o.order_date desc
            ) as rn
        from {{ ref("customer") }} c
        inner join {{ ref("order") }} o on c.customer_key = o.customer_key
    )
select
    ranked_orders.customer_key,
    ranked_orders.customer_name,
    ranked_orders.customer_account_balance,
    ranked_orders.order_date as last_order_date,
    ranked_orders.total_price as last_order_total_price
from ranked_orders
where rn = 1
order by ranked_orders.order_date desc
