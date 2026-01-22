WITH orders AS (
  SELECT
    *
  FROM {{ ref('orders') }}
  LIMIT 25
), my_simple_model AS (
  SELECT
    *
  FROM orders
)
SELECT
  *
FROM my_simple_model