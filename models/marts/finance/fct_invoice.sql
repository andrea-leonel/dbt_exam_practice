with orders_total as (
    
    select * from {{ ref('int_order_totals') }}

),

employees as (

    select * from {{ ref('int_employee_detail') }}
),

companies as (

    select * from {{ ref('int_company_detail') }}
),

invoice_detail as (

    select
    concat(order_date, emp_id) as invoice_id,
    emp_id,
    order_id,
    comp_id,
    total_items as lines_count,
    total_gross_value as invoice_gross,
    total_gross_value + emp_disc_value as invoice_net_before_vat,
    --env.disc_applied,
    --env.emp_disc_value,
    total_vat as invoice_vat_total,
    total_net_value as invoice_total
    from orders_total
    order by order_id

),

invoice_class as (

    select
    *,
    case
        when invoice_total > 1000 then 'high_value'
        when invoice_total between 500 and 1000 then 'mid_value'
        else 'low_value'
    end as invoice_value_bucket
    from invoice_detail

),

-- Final CTEs

invoice_final as (

    select
    'invoice' as record_type,
    inv.invoice_id,
    inv.comp_id,
    c.comp_name,
    inv.emp_id,
    e.first_name,
    e.last_name,
    e.email,
    e.phone_number,
    e.address_city,
    e.address_state,
    e.address_zipcode,
    null as prod_id,
    null as prod_name,
    null as category,
    inv.lines_count,
    round(cast(inv.invoice_gross as numeric),2,'ROUND_HALF_AWAY_FROM_ZERO') as invoice_gross,
    round(cast(inv.invoice_net_before_vat as numeric),2,'ROUND_HALF_AWAY_FROM_ZERO') as invoice_net_before_vat,
    round(cast(inv.invoice_vat_total as numeric),2,'ROUND_HALF_AWAY_FROM_ZERO') as invoice_vat_total,
    round(cast(inv.invoice_total as numeric),2,'ROUND_HALF_AWAY_FROM_ZERO') as invoice_total,
    inv.invoice_value_bucket,
    current_timestamp() as generated_at
    from invoice_class inv
    left join companies c on inv.comp_id = c.comp_id
    left join employees e on inv.emp_id = e.emp_id
    order by record_type, invoice_total desc nulls last, generated_at desc

)

-- Simple SELECT statement.

select * from invoice_final
--where invoice_id = '9801a4b4307121cc6da51c1fc80339db'