WITH stg_orders AS (
  SELECT
    *
  FROM {{ ref('stg_orders') }}
), stg_customers AS (
  SELECT
    *
  FROM {{ ref('stg_customers') }}
), join_1 AS (
  SELECT
    *
  FROM stg_customers
  JOIN stg_orders
    ON stg_customers.CUSTOMER_ID = stg_orders.CUSTOMER_ID
), aggregation_1 AS (
  SELECT
    CUSTOMER_NAME,
    SUM(ORDER_TOTAL) AS sum_ORDER_TOTAL
  FROM join_1
  GROUP BY
    CUSTOMER_NAME
)
SELECT
  *
FROM aggregation_1