WITH stg_supplies_demo AS (
  SELECT
    *
  FROM {{ ref('stg_supplies_demo') }}
), untitled_3_sql AS (
  SELECT
    *
  FROM stg_supplies_demo
)
SELECT
  *
FROM untitled_3_sql