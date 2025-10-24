WITH stging_orders AS (
  SELECT
    *
  FROM {{ ref('stging_orders') }}
), untitled_sql AS (
  SELECT
    *
  FROM stging_orders
)
SELECT
  *
FROM untitled_sql