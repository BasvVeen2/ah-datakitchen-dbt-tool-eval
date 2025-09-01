{{ 
    config(
        materialized="incremental",
        unique_key = "part_key",
        incremental_strategy='merge',
        tblproperties = {'delta.enableChangeDataFeed': 'true'},
        schema="intermediate",
        file_format="delta"
        ) 
}}

-- WITH cdf_customer
-- AS
-- (
-- SELECT 
--     *
--   , MIN(_commit_version) over (PARTITION BY product_id) AS min_commit_version
--   , MAX(_commit_version) over (PARTITION BY product_id) AS max_commit_version
-- FROM table_changes('alh_nonprd_soubhik.dbt_datakitchen_prd_staging.part_nested', 2)
-- ),
-- previous_state
-- AS
-- (
-- SELECT *
-- EXCEPT(_commit_version, _change_type, _commit_timestamp, min_commit_version, max_commit_version)
-- FROM cdf_customer
-- WHERE _commit_version = min_commit_version
--   AND _change_type IN ('delete', 'update_preimage')
-- ),
-- current_state
-- AS
-- (
-- SELECT * 
-- EXCEPT(_commit_version, _change_type, _commit_timestamp, min_commit_version, max_commit_version)
-- FROM cdf_customer
-- WHERE _commit_version = max_commit_version
--   AND _change_type IN ('insert', 'update_postimage')
-- ),
-- SELECT 
--     IFNULL(cs.part_key, ps.part_key) part_key,
--     cs.part_name,
--     cs.manufacturer,
--     cs.brand,
--     cs.type,
--     cs.size,
--     cs.container,
--     cs.retail_price,
--     cs.comment,
--     cs.dimension,
--     cs.shape,
--     cs.dimension_metric.
--     cs.material_category,
--     cs.supplier_key,
--     cs.supplier_name,
--     cs.available_quantity,
--     cs.supply_cost,
--     cs.supplier_address,
--     cs.supplier_phone,
--     cs.supplier_cost_tier,
--     cs.size_category,
--     cs.price_category
--     iff(cs.customer_key IS NULL, TRUE, FALSE) AS _delete
-- FROM previous_state ps
--     FULL JOIN current_state cs ON ps.customer_key = cs.customer_key
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
    END as price_category,
    current_timestamp() as last_modified
FROM {{ ref('part_nested') }} pn
LATERAL VIEW EXPLODE(ps.suppliers) suppliers_table AS supplier

{% if is_incremental() %}
    WHERE pn.last_modified > (select max(last_modified) from {{ this }})
{% endif %}

-- {% else %}
