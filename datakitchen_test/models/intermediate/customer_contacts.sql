{{ 
    config(
        materialized="incremental",
        unique_key = "customer_key"
        incremental_strategy='merge',
        schema="intermediate",
        file_format="delta"
        ) 
}}

select
    customer_key,
    contact.type as contact_type,
    contact.value as contact_value,
    contact.is_primary as is_primary_contact_type,
    last_modified
from {{ ref("customer") }}
lateral view explode(contact_methods) contact_table as contact

{% if is_incremental() %}
  where last_modified > (select max(last_modified) from {{ this }})
{% endif %}