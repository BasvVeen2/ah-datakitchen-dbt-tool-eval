{{ 
    config(
        materialized='incremental',
        incremental_strategy="merge",
        unique_key=['region_key'],
        target_alias='t',
        source_alias='s',
        matched_condition='(t.region_name <> s.region_name OR t.r_comment <> s.r_comment)',

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