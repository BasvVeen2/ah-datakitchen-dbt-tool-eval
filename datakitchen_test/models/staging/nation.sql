{{ config(
    materialized='view',
    schema='staging',
    file_format='delta',
    tblproperties = {'delta.enableChangeDataFeed': 'true'},
    databricks_compute='xxs'
) }}

SELECT
    n_nationkey as nation_key,
    n_name as nation_name,
    n_regionkey as region_key,
    n_comment as comment
FROM {{source("tpch", "nation")}}
