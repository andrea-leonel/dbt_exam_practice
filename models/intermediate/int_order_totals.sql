with orders as (

    select * from {{ ref('int_order_detail') }}
),

comp_orders as (

    select
    comp_id, 
    count(*) as company_orders
    from orders
    group by comp_id

),

emp_orders as (

    select
    emp_id, 
    count(*) as employee_orders
    from orders
    group by emp_id

),

aggregations as (

    select
    o.order_id,
    o.emp_id,
    eo.employee_orders,
    o.comp_id,
    co.company_orders,
    o.order_date,
    sum(o.num_items) as total_items,
    sum(o.product_value) as total_gross_value,
    sum(num_items) as total_num_items
    from orders o
    left join emp_orders eo on o.emp_id = eo.emp_id
    left join comp_orders co on o.comp_id = co.comp_id
    group by o.order_id, o.emp_id, eo.employee_orders, o.comp_id, co.company_orders, o.order_date

)

select * from aggregations