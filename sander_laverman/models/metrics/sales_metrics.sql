SELECT
  d.region,
  order_date,
  COUNT(DISTINCT order_key) as orders_count,
  CAST(SUM(quantity) AS decimal(15,2)) as total_quantity,
  CAST(SUM(extended_price) AS decimal(15,2)) as gross_sales,
  CAST(SUM(discounted_price) AS decimal(15,2)) as net_sales,
  CAST(SUM(final_price) AS decimal(15,2)) as total_revenue,
  CAST(SUM(gross_profit) AS decimal(15,2)) as total_gross_profit,
  CAST(AVG(discount) AS decimal(5,2)) as avg_discount_rate,
  AVG(order_to_ship_days) as avg_fulfillment_days
FROM {{ ref("fact_sales")}} f
INNER JOIN {{ ref("dim_customer")}} d
    ON f.customer_key = d.customer_key
GROUP BY d.region, order_date
