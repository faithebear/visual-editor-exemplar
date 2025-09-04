with

customers as (

    select 
        customer_id,
        customer_name 
    from 
        {{ ref('stg_customers') }}

),

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
        ordered_at,
        order_cost,
        order_items_subtotal,
        count_food_items,
        count_drink_items,
        count_order_items,
        is_food_order,
        is_drink_order,
        customer_order_number * 2
    from 
        {{ ref('orders') }}

),

customer_orders_summary as (

    select
        orders.customer_id,

        count(distinct orders.order_id) as count_lifetime_orders,
        count(distinct orders.order_id) > 1 as is_repeat_buyer,
        min(orders.ordered_at) as first_ordered_at,
        max(orders.ordered_at) as last_ordered_at,
        sum(orders.subtotal) as lifetime_spend_pretax,
        sum(orders.tax_paid) as lifetime_tax_paid,
        sum(orders.order_total) as lifetime_spend

    from orders

    group by 1

),

joined as (

    select
        customers.customer_id,
        customers.customer_name,
        
        customer_orders_summary.count_lifetime_orders,
        customer_orders_summary.first_ordered_at,
        customer_orders_summary.last_ordered_at,
        customer_orders_summary.lifetime_spend_pretax,
        customer_orders_summary.lifetime_tax_paid,
        customer_orders_summary.lifetime_spend,

        case
            when customer_orders_summary.is_repeat_buyer then 'returning'
            else 'new'
        end as customer_type

    from customers

    left join customer_orders_summary
        on customers.customer_id = customer_orders_summary.customer_id

)

select * from joined
