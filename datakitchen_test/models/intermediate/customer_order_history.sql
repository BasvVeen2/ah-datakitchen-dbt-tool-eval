WITH customer_orders AS (
  SELECT
    oli.customer_key,
    oli.order_key,
    oli.order_date,
    oli.final_price,
    oli.quantity,
    oli.part_key,
    ROW_NUMBER() OVER (PARTITION BY oli.customer_key ORDER BY oli.order_date) as order_sequence,
    COUNT(*) OVER (PARTITION BY oli.customer_key) as total_orders,
    SUM(oli.final_price) OVER (
      PARTITION BY oli.customer_key
      ORDER BY oli.order_date
      ROWS UNBOUNDED PRECEDING
    ) as cumulative_spend,
    LAG(oli.order_date) OVER (
      PARTITION BY oli.customer_key
      ORDER BY oli.order_date
    ) as previous_order_date,
    LEAD(oli.order_date) OVER (
      PARTITION BY oli.customer_key
      ORDER BY oli.order_date
    ) as next_order_date
  FROM {{ ref('order_lineitems') }} oli
),

customer_order_patterns AS (
  SELECT
    *,
    DATEDIFF(order_date, previous_order_date) as days_since_last_order,
    DATEDIFF(next_order_date, order_date) as days_to_next_order,
    AVG(DATEDIFF(order_date, previous_order_date)) OVER (
      PARTITION BY customer_key
    ) as avg_order_frequency_days,
    -- Calculate moving averages
    AVG(final_price) OVER (
      PARTITION BY customer_key
      ORDER BY order_date
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) as moving_avg_order_value_3,
    -- Customer lifecycle stage
    CASE
      WHEN order_sequence = 1 THEN 'New'
      WHEN order_sequence <= 5 THEN 'Growing'
      WHEN order_sequence <= 15 THEN 'Mature'
      ELSE 'Loyal'
    END as lifecycle_stage
  FROM customer_orders
)

SELECT
  order_key,
  customer_key,
  order_date,
  final_price,
  quantity,
  part_key,
  total_orders,
  CAST(cumulative_spend AS decimal(15,2)) AS cumulative_spend,
  days_since_last_order,
  days_to_next_order,
  avg_order_frequency_days,
  CAST(moving_avg_order_value_3 AS decimal(15,2)) AS moving_avg_order_value_3,
  lifecycle_stage,
  CASE
    WHEN avg_order_frequency_days <= 30 THEN 'Frequent'
    WHEN avg_order_frequency_days <= 90 THEN 'Regular'
    WHEN avg_order_frequency_days <= 180 THEN 'Occasional'
    ELSE 'Infrequent'
  END as order_frequency_category,
  -- Flag potential churners
  CASE
    WHEN days_since_last_order > avg_order_frequency_days * 2
         AND next_order_date IS NULL
    THEN true
    ELSE false
  END as potential_churn_flag
FROM customer_order_patterns
