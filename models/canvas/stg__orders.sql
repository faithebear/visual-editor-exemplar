WITH raw_orders AS (
  /* One record per order (consisting of one or more order items) */
  SELECT
    *
  FROM {{ source('ecom', 'raw_orders') }}
), projection AS (
  SELECT
    *
    RENAME (CUSTOMER AS CUSTOMER_ID, ID AS ORDER_ID)
  FROM raw_orders
), stg__orders_sql AS (
  SELECT
    *
  FROM projection
)
SELECT
  *
FROM stg__orders_sql