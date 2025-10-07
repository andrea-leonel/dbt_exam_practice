with orders as (

    select * from {{ ref('int_order_detail') }}

),

prod_order as (

    select 
    prod_id,
    prod_name,
    order_date,
    count(distinct order_id) as num_orders,
    sum(num_items) as num_items,
    count(distinct comp_id) as num_comps,
    round(sum(product_value),2) as product_value
    from orders
    group by prod_id, prod_name, order_date
),

date_spine as (

    select 
    a.*,
    b.week_day, 
    b.week_num, 
    b.month_num, 
    b.quarter_num, 
    b.year
    from prod_order a
    left join {{ ref('dim_date') }} b on a.order_date = b.date

)

select * from date_spine