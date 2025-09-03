WITH customers AS (
  /* Customer overview data mart, offering key details for each unique customer. One row per customer. */
  SELECT
    *
  FROM {{ ref('jaffle_shop', 'customers') }}
), orders AS (
  /* Order overview data mart, offering key details for each order inlcluding if it's a customer's first order and a food vs. drink item breakdown. One row per order. */
  SELECT
    *
  FROM {{ ref('jaffle_shop', 'orders') }}
), "join" AS (
  SELECT
    *
  FROM orders
  JOIN customers
    USING (CUSTOMER_ID)
), filter_1 AS (
  SELECT
    *
  FROM "join"
  WHERE
    ORDER_TOTAL > 100
), large_customers_sql AS (
  SELECT
    *
  FROM filter_1
)
SELECT
  *
FROM large_customers_sql