with orders as (

    select * from {{ ref('int_order_totals') }}

),

comp_order as (

    select 
    comp_id,
    order_date,
    count(distinct order_id) as num_orders,
    sum(total_num_items) as num_items,
    count(distinct emp_id) as num_emp,
    round(sum(total_gross_value),2) as order_value_gross
    from orders
    group by comp_id, order_date
),

date_spine as (

    select 
    a.*,
    b.week_day, 
    b.week_num, 
    b.month_num, 
    b.quarter_num, 
    b.year
    from comp_order a
    left join {{ ref('dim_date') }} b on a.order_date = b.date

)

select * from date_spine