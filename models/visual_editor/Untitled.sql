WITH customers AS (
  SELECT
    *
  FROM {{ ref('customers') }}
), filter_1 AS (
  SELECT
    *
  FROM customers
  WHERE
    CUSTOMER_NAME = 'Stephanie'
), aggregation_1 AS (
  SELECT
    COUNT(CUSTOMER_ID) AS count_CUSTOMER_ID,
    MAX(LIFETIME_TAX_PAID) AS max_LIFETIME_TAX_PAID
  FROM filter_1
), untitled_sql AS (
  SELECT
    *
  FROM aggregation_1
)
SELECT
  *
FROM untitled_sql