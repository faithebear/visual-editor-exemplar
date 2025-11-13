WITH staging_orders AS (
  SELECT
    *
  FROM {{ ref('staging_orders') }}
), untitled_sql AS (
  SELECT
    *
  FROM staging_orders
)
SELECT
  *
FROM untitled_sql