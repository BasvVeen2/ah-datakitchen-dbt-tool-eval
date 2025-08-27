{{ config(materialized="view", schema="intermediate", file_format="delta") }}

SELECT
    customer_key,
    contact.type as contact_type,
    contact.value as contact_value,
    contact.is_primary as is_primary_contact_type
from {{ ref("customer") }}
LATERAL VIEW EXPLODE(contact_methods) contact_table AS contact
