{{ 
    config(
        materialized="view",
        schema="intermediate",
        file_format='delta',
        tblproperties = {'delta.enableChangeDataFeed': 'true'}
        ) 
}}

WITH customer_address AS (
  SELECT
    customer_key,
    customer_name,
    nation_key,
    phone,
    account_balance,
    market_segment,
    comment,
    -- Unnesting address structure
    address.street_number,
    address.street_name,
    address.location.regionkey,
    address.location.countrykey,
    address.location.postal_code
  FROM {{ ref('customer') }}
)

SELECT * FROM customer_address