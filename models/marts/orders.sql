WITH stg_orders AS (
  SELECT
    order_id,
    location_id,
    customer_id,
    subtotal_cents,
    tax_paid_cents,
    order_total_cents,
    subtotal,
    tax_paid,
    order_total,
    ordered_at
  FROM {{ ref('stg_orders') }}
), order_items AS (
  SELECT
    order_item_id,
    order_id,
    product_id,
    ordered_at,
    product_name,
    product_price,
    is_food_item,
    is_drink_item,
    supply_cost
  FROM {{ ref('order_items') }}
), formula_1 AS (
  SELECT
    *,
    CASE WHEN is_food_item THEN 1 ELSE 0 END AS agg__e72afe53_1998_4de7_89d7_ec22592b3943,
    CASE WHEN is_drink_item THEN 1 ELSE 0 END AS agg__20b4c84e_ed1f_4e37_a27b_0c6b0b1591a3
  FROM order_items
), aggregation_1 AS (
  SELECT
    order_id,
    SUM(supply_cost) AS order_cost,
    SUM(product_price) AS order_items_subtotal,
    COUNT(order_item_id) AS count_order_items,
    SUM(agg__e72afe53_1998_4de7_89d7_ec22592b3943) AS count_food_items,
    SUM(agg__20b4c84e_ed1f_4e37_a27b_0c6b0b1591a3) AS count_drink_items
  FROM formula_1
  GROUP BY
    order_id
), join_1 AS (
  SELECT
    *
  FROM stg_orders
  LEFT JOIN aggregation_1
    ON stg_orders.order_id = aggregation_1.order_id
), formula_2 AS (
  SELECT
    *,
    count_food_items > 0 AS is_food_order,
    count_drink_items > 0 AS is_drink_order
  FROM join_1
), formula_3 AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY ordered_at ASC) AS customer_order_number
  FROM formula_2
), order_1 AS (
  SELECT
    *
  FROM formula_3
  ORDER BY
    ordered_at ASC
)
SELECT
  *
FROM order_1