{{ config(
    materialized='view',
    file_format='delta',
    access="public"
) }}

select * from {{ ref("dk_dbt_tooleval", "customer") }}