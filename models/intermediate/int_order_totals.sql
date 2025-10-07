with orders as (

    select * from {{ ref('int_order_detail') }}
),

employee_details as (

    select * from {{ ref('int_employee_detail') }}
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
    sum(num_items) as total_num_items,
    sum(vat_value) as total_vat
    from orders o
    left join emp_orders eo on o.emp_id = eo.emp_id
    left join comp_orders co on o.comp_id = co.comp_id
    group by o.order_id, o.emp_id, eo.employee_orders, o.comp_id, co.company_orders, o.order_date

),

discount_emp as (

    select
    o.*,
    case
        when o.employee_orders >= 100 then 0.05
        when o.employee_orders >= 50 then 0.03
        when o.employee_orders >= 10 then 0.01
        else 0.00
    end as emp_disc,
    case
        when date_diff(current_date(), c.comp_dateadded, year)>= 5 then 0.10
        when date_diff(current_date(), c.comp_dateadded, year)>= 2 then 0.05
        when o.company_orders > 200 then 0.04
        else 0.00
    end as comp_disc
    from aggregations o
    left join employee_details c on o.emp_id = c.emp_id
),

emp_net_value as (

    select
    o.*,
    greatest(emp_disc,comp_disc) as disc_applied,
    o.total_gross_value * greatest(emp_disc,comp_disc) * -1 as emp_disc_value
    from discount_emp o

),

total_net_value as (

    select
    *,
    total_gross_value + total_vat + emp_disc_value as total_net_value
    from emp_net_value
)

select * from total_net_value