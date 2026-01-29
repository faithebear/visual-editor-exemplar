WITH products AS (
  SELECT
    *
  FROM {{ ref('jaffle_shop', 'products') }}
), stg_supplies_demo AS (
  SELECT
    *
  FROM {{ ref('stg_supplies_demo') }}
), join_1 AS (
  SELECT
    stg_supplies_demo.supply_id,
    stg_supplies_demo.supply_name,
    stg_supplies_demo.supply_cost_usd,
    stg_supplies_demo.is_perishable,
    products.PRODUCT_ID,
    products.PRODUCT_NAME,
    products.PRODUCT_TYPE,
    products.PRODUCT_DESCRIPTION,
    products.PRODUCT_PRICE,
    products.IS_FOOD_ITEM,
    products.IS_DRINK_ITEM
  FROM stg_supplies_demo
  JOIN products
    ON stg_supplies_demo.product_id = products.PRODUCT_ID
), aggregation AS (
  SELECT
    IS_FOOD_ITEM,
    IS_DRINK_ITEM,
    AVG(PRODUCT_PRICE) AS avg_PRODUCT_PRICE
  FROM join_1
  GROUP BY
    IS_FOOD_ITEM,
    IS_DRINK_ITEM
), avg_food_vs_drink_cost_sql AS (
  SELECT
    *
  FROM aggregation
)
SELECT
  *
FROM avg_food_vs_drink_cost_sql