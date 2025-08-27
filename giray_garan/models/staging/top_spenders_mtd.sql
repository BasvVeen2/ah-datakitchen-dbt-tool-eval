SELECT 
    c.customer_name,
    SUM(o.total_price) AS total_order_amount
FROM 
    {{ ref('customer') }} AS c
INNER JOIN 
    {{ ref('order') }} AS o
ON 
    c.customer_key = o.customer_key
WHERE 
    o.order_date >= date_trunc('month', current_date())
GROUP BY 
    c.customer_name
ORDER BY 
    total_order_amount DESC