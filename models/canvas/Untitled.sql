WITH locations AS (
  SELECT
    *
  FROM {{ ref('jaffle_shop', 'locations') }}
), untitled_sql AS (
  SELECT
    *
  FROM locations
)
SELECT
  *
FROM untitled_sql