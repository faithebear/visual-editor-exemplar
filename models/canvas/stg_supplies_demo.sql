WITH raw_supplies AS (
  /* One record per supply per SKU of items sold in stores */
  SELECT
    *
  FROM {{ source('ecom', 'raw_supplies') }}
), rename_columns AS (
  SELECT
    *
    RENAME (ID AS supply_id, NAME AS supply_name, COST AS supply_cost_usd, PERISHABLE AS is_perishable, SKU AS product_id)
  FROM raw_supplies
), stg_supplies_demo_sql AS (
  SELECT
    *
  FROM rename_columns
)
SELECT
  *
FROM stg_supplies_demo_sql