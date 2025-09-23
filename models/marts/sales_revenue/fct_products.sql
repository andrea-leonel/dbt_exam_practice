with products as (

    select * from {{ ref('stg_products') }}

),

aggregations as (

    select 
    a.prod_id,
    max(b.order_date) as latest_order_date,
    min(b.order_date) as first_order_date,
    count(distinct b.order_id) as num_orders,
    max(b.order_value) as max_order_value,
    min(b.order_value) as min_order_value,
    round(sum(b.order_value),2) as lifetime_value,
    max(b.num_items) as max_order_basket,
    min(b.num_items) as min_order_basket
    from products a
    left join {{ ref('int_order_detail') }} b on a.prod_id = b.prod_id
    group by a.prod_id

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