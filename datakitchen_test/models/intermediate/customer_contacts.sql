{{ 
    config(
        materialized="incremental",
        unique_key = ['customer_key', 'contact_type'],
        incremental_strategy='merge',
        schema="intermediate",
        file_format='delta',
        tblproperties = {'delta.enableChangeDataFeed': 'true'},
        databricks_compute='small'
) }}


select
    c.customer_key,
    contact.type as contact_type,
    contact.value as contact_value,
    contact.is_primary as is_primary_contact_type,
    current_timestamp() as last_modified
from {{ ref("customer") }} c
lateral view explode(contact_methods) contact_table as contact

{% if is_incremental() %}
    WHERE c.last_modified > (select max(last_modified) from {{ this }})
{% endif %}
