WITH products AS (
  SELECT
    PRODUCT_ID,
    PRODUCT_NAME,
    IS_FOOD_ITEM
  FROM {{ ref('jaffle_shop', 'products') }}
), untitled_sql AS (
  SELECT
    *
  FROM products
)
SELECT
  *
FROM untitled_sql