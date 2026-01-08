SELECT 
    DATE_TRUNC('month', ordered_at) AS order_month,
    SUM(order_total) AS order_total_sum
FROM 
    {{ ref('orders') }}

    {% if is_incremental() %}
        where order_month > (select max(order_month) from {{ this }}) 
    {% endif %}
GROUP BY 
    1
ORDER BY 
    1