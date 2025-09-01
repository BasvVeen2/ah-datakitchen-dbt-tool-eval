{{ 
    config(
        materialized="incremental",
        unique_key = "customer_key",
        incremental_strategy='merge',
        partition_by = ['region_key'],
        schema="marts",
        file_format="delta",
        databricks_compute='medium'
        ) 
}}

SELECT
    cd.customer_key,
    cd.customer_name,
    cd.account_balance,
    cd.market_segment,
    cd.comment,
    cd.street_number,
    cd.street_name,
    r.region_key,
    r.comment AS region_comment,
    n.nation_name as nation,
    n.comment AS nation_comment,
    cd.postal_code,
    -- Get primary contact info
    cc_primary.contact_value as primary_phone,
    cc_email.contact_value as email_address,
    -- Customer classification
    CASE
        WHEN cd.account_balance > 5000 THEN 'Premium'
        WHEN cd.account_balance > 1000 THEN 'Standard'
        ELSE 'Basic'
    END as customer_tier,
    current_timestamp() as last_modified
FROM {{ ref('customer_details') }} cd
INNER JOIN {{ ref('customer_contacts') }} cc_primary
    ON cd.customer_key = cc_primary.customer_key
    AND cc_primary.contact_type = 'phone'
    AND cc_primary.is_primary_contact_type = true
INNER JOIN {{ ref('customer_contacts') }} cc_email
    ON cd.customer_key = cc_email.customer_key
    AND cc_email.contact_type = 'email'
INNER JOIN {{ ref('region') }} r 
    ON cd.regionkey = r.region_key
INNER JOIN {{ ref('nation') }} n
    ON cd.countrykey = n.nation_key

{% if is_incremental() %}
  where cd.last_modified > (select max(last_modified) from {{ this }})
    OR cc_primary.last_modified > (select max(last_modified) from {{ this }})
{% endif %}
