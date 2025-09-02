{{ 
    config(
        materialized='incremental',
        incremental_strategy="merge",
        schema="staging",
        file_format='delta',
        tblproperties = {'delta.enableChangeDataFeed': 'true'}
)
}}


SELECT
    r_regionkey as region_key,
    r_name as region_name,
    r_comment as comment
FROM {{source("tpch", "region")}}