WITH customer AS (
  /* This model provides a detailed view of customer information, including personal details, contact methods, and account history. It is used to analyze customer demographics, contact preferences, and financial interactions. */
  SELECT
    *
  FROM {{ ref('jose_joao', 'customer') }}
), `order` AS (
  /* This model provides a detailed view of customer orders, including key information such as order status, total price, and shipping details. It is designed to facilitate analysis of order processing and fulfillment. */
  SELECT
    *
  FROM {{ ref('jose_joao', 'order') }}
), `join` AS (
  SELECT
    customer.comment,
    `order`.comment AS comment_1
  FROM customer
  JOIN `order`
    USING (customer_key)
), canvas_model_sql AS (
  SELECT
    *
  FROM `join`
)
SELECT
  *
FROM canvas_model_sql