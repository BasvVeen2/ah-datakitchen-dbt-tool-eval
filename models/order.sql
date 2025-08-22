{{ config(
    materialized='view',
    schema='staging',
    file_format='delta'
) }}


SELECT
    o_orderkey as order_key,
    o_custkey as customer_key,
    o_orderstatus as order_status,
    o_totalprice as total_price,
    o_orderdate as order_date,
    o_orderpriority as order_priority,
    o_clerk as clerk,
    o_shippriority as ship_priority,
    o_comment as comment,
    shipping_info,
    line_items
FROM alh_dk_dbt_test_tpch.raw.orders_nested