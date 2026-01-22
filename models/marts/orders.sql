with

orders as (

    select 
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
    from 
        {{ ref('stg_orders') }}

),

order_items as (

    select 
        order_item_id,
        order_id,
        product_id,
        ordered_at,
        product_name,
        product_price,
        is_food_item,
        is_drink_item,
        supply_cost 
    from 
        {{ ref('order_items') }}

),

order_items_summary as (

    select
        order_id,

        sum(supply_cost) as order_cost,
        sum(product_price) as order_items_subtotal,
        count(order_item_id) as count_order_items,
        sum(
            case
                when is_food_item then 1
                else 0
            end
        ) as count_food_items,
        sum(
            case
                when is_drink_item then 1
                else 0
            end
        ) as count_drink_items

    from order_items

    group by 1

),

compute_booleans as (

    select
        orders.order_id,
        orders.location_id,
        orders.customer_id,
        orders.subtotal_cents,
        orders.tax_paid_cents,
        orders.order_total_cents,
        orders.subtotal,
        orders.tax_paid,
        orders.subtotal - orders.tax_paid as order_profit,
        orders.order_total,
        orders.ordered_at,
        order_items_summary.order_cost,
        order_items_summary.order_items_subtotal,
        order_items_summary.count_food_items,
        order_items_summary.count_drink_items,
        order_items_summary.count_order_items,
        order_items_summary.count_food_items > 0 as is_food_order,
        order_items_summary.count_drink_items > 0 as is_drink_order

    from orders

    left join
        order_items_summary
        on orders.order_id = order_items_summary.order_id

),

customer_order_count as (

    select
        compute_booleans.order_id,
        compute_booleans.location_id,
        compute_booleans.customer_id,
        compute_booleans.subtotal_cents,
        compute_booleans.tax_paid_cents,
        compute_booleans.order_total_cents,
        compute_booleans.subtotal,
        compute_booleans.tax_paid,
        compute_booleans.order_profit,
        compute_booleans.order_total,
        compute_booleans.ordered_at,
        compute_booleans.order_cost + 1 as order_cost_plus_one,
        compute_booleans.order_items_subtotal,
        compute_booleans.count_food_items,
        compute_booleans.count_drink_items,
        compute_booleans.count_order_items,
        compute_booleans.is_food_order,
        compute_booleans.is_drink_order,        

        row_number() over (
            partition by customer_id
            order by ordered_at asc
        ) as customer_order_number

    from compute_booleans

)

select * from customer_order_count