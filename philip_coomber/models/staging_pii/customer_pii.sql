{{ config(
    materialized='view',
    schema='staging_pii',
    file_format='delta'
) }}

select
    c_custkey as customer_key,
    c_name as customer_name,
    c_nationkey as nation_key,
    c_phone as phone,
    c_acctbal as account_balance,
    c_mktsegment as market_segment,
    c_comment as comment,
    address,
    contact_methods,
    account_history
from {{source("tpch", "customer")}}
