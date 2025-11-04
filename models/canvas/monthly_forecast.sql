WITH order_items AS (
  SELECT
    *
  FROM {{ ref('jaffle_shop', 'order_items') }}
), monthly_forecast_sql AS (
  SELECT
    *
  FROM order_items
)
SELECT
  *
FROM monthly_forecast_sql