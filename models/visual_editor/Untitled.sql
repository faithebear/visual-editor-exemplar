WITH customers AS (
  SELECT
    *
  FROM {{ ref('customers') }}
), aggregation_1 AS (
  SELECT
    COUNT(DISTINCT CUSTOMER_ID) AS countd_CUSTOMER_ID
  FROM customers
), untitled_sql AS (
  SELECT
    *
  FROM aggregation_1
)
SELECT
  *
FROM untitled_sql