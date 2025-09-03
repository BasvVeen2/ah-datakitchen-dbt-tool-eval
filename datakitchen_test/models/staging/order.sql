{{ config(
    materialized='incremental',
    incremental_strategy="merge",
    partition_by = ['order_date'],
    unique_key = 'order_key',
    schema='staging',
    file_format='delta',
    tblproperties = {'delta.enableChangeDataFeed': 'true'}
) }}

WITH ranked_orders AS (
    SELECT
        s.o_orderkey as order_key,
        s.o_custkey as customer_key,
        s.o_orderstatus as order_status,
        s.o_totalprice as total_price,
        s.o_orderdate as order_date,
        s.o_orderpriority as order_priority,
        s.o_clerk as clerk,
        s.o_shippriority as ship_priority,
        s.o_comment as comment,
        s.shipping_info,
        s.line_items,
        current_timestamp() as last_modified,
        ROW_NUMBER() OVER (PARTITION BY o_orderkey ORDER BY o_orderdate desc) AS row_num
    FROM {{source("tpch", "order")}} s
        {% if is_incremental() %}
        LEFT OUTER JOIN {{ this}} t ON t.order_key = s.o_orderkey
        WHERE t.order_key IS NULL OR s.o_orderdate > t.order_date
        {% endif %}
)
SELECT
    order_key,
    customer_key,
    order_status,
    total_price,
    order_date,
    order_priority,
    clerk,
    ship_priority,
    comment,
    shipping_info,
    line_items,
    last_modified
FROM ranked_orders
WHERE row_num = 1;