{{ config(
    materialized='view',
    file_format='delta',
    access="public"
) }}

select * from {{ ref("dbt-datakitchen-test", "customer") }}