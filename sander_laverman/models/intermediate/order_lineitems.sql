{{ config(materialized="view", schema="intermediate", file_format="delta") }}

SELECT
    order_key,
    customer_key,
    order_date,
    total_price,
    order_status,
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
    CAST(line_item.extended_price * (1 - line_item.discount) * (1 + line_item.tax) AS decimal(15,2)) as final_price
FROM {{ ref('order') }}
LATERAL VIEW EXPLODE(line_items) line_items_table AS line_item
