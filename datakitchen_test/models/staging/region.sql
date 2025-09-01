{{ 
    config(
        materialized="view",
        schema="staging",
        file_format='delta',
        tblproperties = {'delta.enableChangeDataFeed': 'true'},
        databricks_compute = 'xxs'
)
}}


SELECT
    r_regionkey as region_key,
    r_name as region_name,
    r_comment as comment
FROM {{source("tpch", "region")}}