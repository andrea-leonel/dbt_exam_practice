with companies as (
    
    select * from {{ ref('stg_companies') }}

),

orders_total as (
    
    select * from {{ ref('int_order_totals') }}

),

orders_detail as (
    
    select * from {{ ref('int_order_detail') }}

),

-- Logic CTEs

discount_emp as (

    select
    o.order_id,
    o.emp_id,
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
    from orders_total o
    left join companies c on o.comp_id = c.comp_id
),

emp_net_value as (

    select
    o.order_id,
    o.emp_id,
    o.total_gross_value,
    emp_disc,
    comp_disc,
    greatest(de.emp_disc,de.comp_disc) as disc_applied,
    round(o.total_gross_value * (greatest(de.emp_disc,de.comp_disc)),2) * -1 as emp_disc_value,
    from orders_total o
    left join discount_emp de on o.order_id = de.order_id

),

discount_prod as (

    select
    order_id,
    prod_id,
    product_value,
    case
        when lower(category) like '%food%' then 0.05
        when lower(category) like '%electronics%' then 0.20
        when lower(category) like '%clothing%' then 0.12
        else 0.18
    end as vat_perc
    from orders_detail

),

order_vat as (

    select
    order_id,
    prod_id,
    product_value,
    vat_perc,
    round(safe_multiply(product_value,vat_perc),2) as vat_value
    from discount_prod

),

vat_aggregated as (

    select 
    order_id,
    sum(vat_value) as total_vat
    from order_vat
    group by order_id 
),

invoice_totals as (

    select 
    o.order_id,
    o.order_date,
    o.emp_id,
    o.comp_id,
    o.total_items,
    o.total_gross_value,
    vat.total_vat,
    env.emp_disc_value,
    round(o.total_gross_value + vat.total_vat + env.emp_disc_value,2) as total_net_value
    from orders_total o
    left join vat_aggregated vat on vat.order_id = o.order_id
    left join emp_net_value env on env.order_id = o.order_id

)

select * from invoice_totals