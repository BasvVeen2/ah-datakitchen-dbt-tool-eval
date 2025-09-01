{{ config(materialized="table", schema="intermediate", file_format="delta") }}

select
    customer_key,
    contact.type as contact_type,
    contact.value as contact_value,
    contact.is_primary as is_primary_contact_type
from {{ ref("customer") }}
lateral view explode(contact_methods) contact_table as contact
