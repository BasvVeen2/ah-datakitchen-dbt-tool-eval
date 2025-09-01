{{ 
    config(
        materialized="incremental",
        unique_key = "customer_key",
        incremental_strategy='merge',
        schema="intermediate",
        file_format='delta',
        tblproperties = {'delta.enableChangeDataFeed': 'true'},
        -- target_alias='target',
        -- source_alias='source',
        -- matched_update_condition="source._delete = false AND (target.contact_type <> source.contact_type OR target.contact_value <> source.contact_value OR target.is_primary_contact_type <> source.is_primary_contact_type)",
        -- matched_delete_condition="source._delete = true"
) }}


select
    c.customer_key,
    c.contact.type as contact_type,
    c.contact.value as contact_value,
    c.contact.is_primary as is_primary_contact_type,
    current_timestamp() as last_modified
from {{ ref("customer") }} c
lateral view explode(contact_methods) contact_table as contact

{% if is_incremental() %}
    WHERE c.last_modified > >= (select max(last_modified) from {{ this }})
{% endif %}
-- WITH cdf_customer
-- AS
-- (
-- SELECT 
--     *
--   , MIN(_commit_version) over (PARTITION BY product_id) AS min_commit_version
--   , MAX(_commit_version) over (PARTITION BY product_id) AS max_commit_version
-- FROM table_changes('alh_nonprd_soubhik.dbt_datakitchen_prd_staging.customer', 2)
-- ),
-- previous_state
-- AS
-- (
-- SELECT *
-- EXCEPT(_commit_version, _change_type, _commit_timestamp, min_commit_version, max_commit_version)
-- FROM cdf_customer
-- WHERE _commit_version = min_commit_version
--   AND _change_type IN ('delete', 'update_preimage')
-- ),
-- current_state
-- AS
-- (
-- SELECT * 
-- EXCEPT(_commit_version, _change_type, _commit_timestamp, min_commit_version, max_commit_version)
-- FROM cdf_customer
-- WHERE _commit_version = max_commit_version
--   AND _change_type IN ('insert', 'update_postimage')
-- ),
-- SELECT 
--     IFNULL(cs.customer_key, ps.customer_key) customer_key,
--     cs.contact_type,
--     cs.contact_value,
--     cs.is_primary_contact_type,
--     iff(cs.customer_key IS NULL, TRUE, FALSE) AS _delete
-- FROM previous_state ps
--     FULL JOIN current_state cs ON ps.customer_key = cs.customer_key

-- {% else %}

-- {% endif %}

