WITH customers AS (
  /* Customer overview data mart, offering key details for each unique customer. One row per customer. */
  SELECT
    *
  FROM {{ ref('jaffle_shop', 'customers') }}
), filter_1 AS (
  SELECT
    *
  FROM customers
  WHERE
    LIFETIME_SPEND_PRETAX > 10
), large_customers_sql AS (
  SELECT
    *
  FROM filter_1
)
SELECT
  *
FROM large_customers_sql