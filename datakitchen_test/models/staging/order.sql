{{ config(
    materialized='incremental',
    incremental_strategy="merge",
    partition_by = {'field': 'order_date', 'data_type': 'date'},
    unique_key = 'order_key'
    schema='staging',
    file_format='delta',
    tblproperties = {'delta.enableChangeDataFeed': 'true'}
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
    line_items,
    current_timestamp() as last_modified
    
FROM {{source("tpch", "order")}}

{% if is_incremental() %}
  where order_date > (select max(last_modified) from {{ this }})
{% endif %}