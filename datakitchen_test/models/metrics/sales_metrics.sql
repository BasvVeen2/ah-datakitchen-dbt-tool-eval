WITH daily_sales AS (
  SELECT
    order_date,
    customer_key,
    COUNT(DISTINCT order_key) as orders_count,
    SUM(quantity) as total_quantity,
    SUM(extended_price) as gross_sales,
    SUM(discounted_price) as net_sales,
    SUM(final_price) as total_revenue,
    SUM(gross_profit) as total_gross_profit,
    AVG(discount) as avg_discount_rate,
    AVG(order_to_ship_days) as avg_fulfillment_days
  FROM {{ ref('fact_sales') }}
  GROUP BY order_date, customer_key
),

customer_metrics AS (
  SELECT
    customer_key,
    COUNT(DISTINCT order_date) as active_days,
    SUM(orders_count) as lifetime_orders,
    SUM(total_revenue) as lifetime_value,
    AVG(total_revenue) as avg_daily_revenue,
    MAX(order_date) as last_order_date,
    MIN(order_date) as first_order_date
  FROM daily_sales
  GROUP BY customer_key
)

SELECT
    ds.order_date,
    ds.customer_key,
    ds.orders_count,
    CAST(ds.total_quantity AS decimal(15,2)) AS total_quantity,
    CAST(ds.gross_sales AS decimal(15,2)) AS gross_sales,
    CAST(ds.net_sales AS decimal(15,2)) AS net_sales,
    CAST(ds.total_revenue AS decimal(15,2)) AS total_revenue,
    CAST(ds.total_gross_profit AS decimal(15,2)) AS total_gross_profit,
    CAST(ds.avg_discount_rate AS decimal(5,2)) AS avg_discount_rate,
    ds.avg_fulfillment_days,
    cm.active_days,
    cm.lifetime_orders,
    CAST(cm.lifetime_value AS decimal(15,2)) AS lifetime_value,
    CAST(cm.avg_daily_revenue AS decimal(15,2)) AS avg_daily_revenue,
    cm.first_order_date,
    cm.last_order_date,
    -- Customer segmentation
    CASE
        WHEN cm.lifetime_value > 50000 THEN 'High Value'
        WHEN cm.lifetime_value > 10000 THEN 'Medium Value'
        ELSE 'Low Value'
    END as customer_segment,
    -- Recency calculation
    DATEDIFF(CURRENT_DATE(), cm.last_order_date) as days_since_last_order
FROM daily_sales ds
JOIN customer_metrics cm ON ds.customer_key = cm.customer_key
