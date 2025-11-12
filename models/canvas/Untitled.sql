WITH stg__orders AS (
  SELECT
    *
  FROM {{ ref('stg__orders') }}
), untitled_sql AS (
  SELECT
    *
  FROM stg__orders
)
SELECT
  *
FROM untitled_sql