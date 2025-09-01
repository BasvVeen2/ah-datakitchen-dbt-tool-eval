{{ 
    config(
        materialized="incremental",
        unique_key = "customer_key",
        incremental_strategy='merge',
        schema="intermediate",
        file_format='delta',
        tblproperties = {'delta.enableChangeDataFeed': 'true'}
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
