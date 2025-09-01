{{ 
    config(
        materialized="incremental",
        unique_key = ['part_key', 'supplier_key'],
        incremental_strategy='merge',
        tblproperties = {'delta.enableChangeDataFeed': 'true'},
        schema="intermediate",
        file_format="delta",
        databricks_compute='small'
        ) 
}}

SELECT
    pn.part_key,
    pn.part_name,
    pn.manufacturer,
    pn.brand,
    pn.type,
    pn.size,
    pn.container,
    pn.retail_price,
    pn.comment,
    -- Unnesting specifications
    pn.specifications.physical.dimension,
    pn.specifications.physical.shape,
    pn.specifications.physical.dimension_metric,
    pn.specifications.material.category as material_category,
    -- Unnesting suppliers array
    supplier.supplier_key,
    supplier.supplier_name,
    supplier.available_quantity,
    supplier.supply_cost,
    supplier.supplier_details.address as supplier_address,
    supplier.supplier_details.phone as supplier_phone,
    supplier.supplier_details.cost_tier as supplier_cost_tier,
    CASE
        WHEN pn.size <= 10 THEN 'Small'
        WHEN pn.size <= 30 THEN 'Medium'
        WHEN pn.size <= 50 THEN 'Large'
        ELSE 'Extra Large'
    END as size_category,
    CASE
        WHEN pn.retail_price <= 1000 THEN 'Budget'
        WHEN pn.retail_price <= 5000 THEN 'Standard'
        WHEN pn.retail_price <= 10000 THEN 'Premium'
        ELSE 'Luxury'
    END as price_category,
    current_timestamp() as last_modified
FROM {{ ref('part_nested') }} pn
LATERAL VIEW EXPLODE(pn.suppliers) suppliers_table AS supplier

{% if is_incremental() %}
    WHERE pn.last_modified > (select max(last_modified) from {{ this }})
{% endif %}