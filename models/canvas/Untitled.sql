WITH input AS (
  SELECT
    *
  FROM {{ ref('jaffle_shop', 'orders') }}
), untitled_sql AS (
  SELECT
    *
  FROM input
)
SELECT
  *
FROM untitled_sql