WITH raw_orders AS (
  /* One record per order (consisting of one or more order items) */
  SELECT
    *
  FROM {{ source('ecom', 'raw_orders') }}
), renaming AS (
  SELECT
    *
    RENAME (ID AS order_id, CUSTOMER AS customer_id, ORDERED_AT AS ordered_at_timestamp)
  FROM raw_orders
), staging_orders_sql AS (
  SELECT
    *
  FROM renaming
)
SELECT
  *
FROM staging_orders_sql