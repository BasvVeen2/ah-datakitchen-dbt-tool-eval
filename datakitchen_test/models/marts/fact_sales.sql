SELECT
    oli.order_key,
    oli.customer_key,
    oli.part_key,
    oli.supplier_key,
    oli.order_date,
    oli.ship_date,
    oli.quantity,
    oli.extended_price,
    oli.discount,
    oli.tax,
    oli.discounted_price,
    oli.final_price,
    oli.transit_days,
    -- Additional metrics
    CAST(oli.final_price - ps.supply_cost * oli.quantity AS decimal(15,2)) as gross_profit,
    DATEDIFF(oli.ship_date, oli.order_date) as order_to_ship_days
FROM {{ ref('order_lineitems') }} oli
LEFT JOIN {{ ref('part_supplier') }} ps
    ON oli.part_key = ps.part_key
    AND oli.supplier_key = ps.supplier_key
