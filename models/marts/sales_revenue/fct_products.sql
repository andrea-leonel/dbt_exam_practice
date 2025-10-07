with orders_detail as (

    select * from {{ ref('int_order_detail') }}

),

aggregations as (

    select 
    distinct prod_id,
    max(order_date) as latest_order_date,
    min(order_date) as first_order_date,
    count(distinct order_id) as num_orders,
    max(product_value) as max_order_value,
    min(product_value) as min_order_value,
    round(sum(product_value),2) as lifetime_value,
    max(num_items) as max_order_basket,
    min(num_items) as min_order_basket
    from orders_detail
    group by prod_id

),

status as (

    select 
    *,
    case
        when num_orders = 0 then 'Never ordered'
        when num_orders = 1 then 'Single order'
        when num_orders > 1 then 'Multiple orders' else null
    end as status,
    from aggregations 
)

select * from status