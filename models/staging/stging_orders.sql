WITH raw_orders AS (
  /* One record per order (consisting of one or more order items) */
  SELECT
    *
  FROM {{ source('ecom', 'raw_orders') }}
), projection AS (
  SELECT
    *
    RENAME (ID AS order_id, SUBTOTAL AS order_subtotal, CUSTOMER AS customer_id)
  FROM raw_orders
), stging_orders_sql AS (
  SELECT
    *
  FROM projection
)
SELECT
  *
FROM stging_orders_sql