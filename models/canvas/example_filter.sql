WITH orders AS (
  /* Order overview data mart, offering key details for each order inlcluding if it's a customer's first order and a food vs. drink item breakdown. One row per order. */
  SELECT
    *
  FROM {{ ref('jaffle_shop', 'orders') }}
), filter_1 AS (
  SELECT
    *
  FROM orders
  WHERE
    TAX_PAID_CENTS <= 24
), example_filter_sql AS (
  SELECT
    *
  FROM filter_1
)
SELECT
  *
FROM example_filter_sql