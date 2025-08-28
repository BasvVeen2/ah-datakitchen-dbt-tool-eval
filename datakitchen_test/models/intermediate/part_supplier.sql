{{ 
    config(
        materialized="incremental",
        unique_key = "part_key",
        incremental_strategy='merge',
        schema="intermediate",
        file_format="delta"
        ) 
}}

SELECT
    ps.part_key,
    ps.part_name,
    ps.manufacturer,
    ps.brand,
    ps.type,
    ps.size,
    ps.container,
    ps.retail_price,
    ps.comment,
    -- Unnesting specifications
    ps.specifications.physical.dimension,
    ps.specifications.physical.shape,
    ps.specifications.physical.dimension_metric,
    ps.specifications.material.category as material_category,
    -- Unnesting suppliers array
    supplier.supplier_key,
    supplier.supplier_name,
    supplier.available_quantity,
    supplier.supply_cost,
    supplier.supplier_details.address as supplier_address,
    supplier.supplier_details.phone as supplier_phone,
    supplier.supplier_details.cost_tier as supplier_cost_tier,
    CASE
        WHEN ps.size <= 10 THEN 'Small'
        WHEN ps.size <= 30 THEN 'Medium'
        WHEN ps.size <= 50 THEN 'Large'
        ELSE 'Extra Large'
    END as size_category,
    CASE
        WHEN ps.retail_price <= 1000 THEN 'Budget'
        WHEN ps.retail_price <= 5000 THEN 'Standard'
        WHEN ps.retail_price <= 10000 THEN 'Premium'
        ELSE 'Luxury'
    END as price_category
FROM {{ ref('part_nested') }} ps
LATERAL VIEW EXPLODE(ps.suppliers) suppliers_table AS supplier

{% if is_incremental() %}
  where updated_at > (select max(updated_at) from {{ this }})
{% endif %}