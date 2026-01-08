SELECT 
    DATE_TRUNC('month', ordered_at) AS order_month,
    SUM(order_total) AS order_total_sum
FROM 
    {{ ref('orders') }}
GROUP BY 
    1
ORDER BY 
    1