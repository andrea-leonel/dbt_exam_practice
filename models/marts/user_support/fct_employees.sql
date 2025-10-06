with employees as (

    select * from {{ ref('int_employee_detail') }}

),

order_detail as (

    select 
    a.emp_id,
    a.comp_id,
    a.prod_most_ordered,
    max(b.order_date) as latest_order_date,
    min(b.order_date) as first_order_date,
    count(distinct b.order_id) as num_orders,
    max(b.total_gross_value) as max_order_gross,
    min(b.total_gross_value) as min_order_gross,
    round(avg(b.total_gross_value),2) as avg_order_gross,
    max(b.total_items) as max_order_basket,
    min(b.total_items) as min_order_basket
    from employees a
    left join {{ ref('int_order_totals') }} b on a.emp_id = b.emp_id
    group by a.emp_id, a.comp_id, prod_most_ordered

),

join_all as (

    select 
    emp_id,
    comp_id,
    case
        when num_orders = 0 then 'Never ordered'
        when num_orders = 1 then 'Single order'
        when num_orders > 1 then 'Multiple orders' else null
    end as status,
    latest_order_date,
    first_order_date,
    num_orders,
    max_order_gross,
    min_order_gross,
    avg_order_gross,
    max_order_basket,
    min_order_basket,
    prod_most_ordered
    from order_detail
)

select * from join_all