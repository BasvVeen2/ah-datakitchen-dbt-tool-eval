{{ 
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by = ['order_date'],
        schema="intermediate",
        tblproperties = {'delta.enableChangeDataFeed': 'true'},
        file_format="delta"
        ) 
}}

SELECT
    o.order_key,
    o.customer_key,
    o.order_date,
    o.total_price,
    o.order_status,
    -- Unnesting line items array
    line_item.part_key,
    line_item.supplier_key,
    line_item.linenumber,
    line_item.quantity,
    line_item.extended_price,
    line_item.discount,
    line_item.tax,
    line_item.line_status,
    line_item.ship_date,
    line_item.shipment_details.mode as ship_mode,
    line_item.shipment_details.receipt_date,
    line_item.shipment_details.transit_days,
    -- Calculate derived fields
    CAST(line_item.extended_price * (1 - line_item.discount) AS decimal(15,2)) as discounted_price,
    CAST(line_item.extended_price * (1 - line_item.discount) * (1 + line_item.tax) AS decimal(15,2)) as final_price,
    current_timestamp() as last_modified
FROM {{ ref('order') }} o
LATERAL VIEW EXPLODE(line_items) line_items_table AS line_item

{% if is_incremental() %}
    WHERE o.last_modified > (select max(last_modified) from {{ this }})
{% endif %}