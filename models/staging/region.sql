{{ config(materialized="view", schema="staging", file_format="delta") }}


SELECT
    r_regionkey as region_key,
    r_name as region_name,
    r_comment as comment
from alh_dk_dbt_test_tpch.raw.region
