with employees as (
    
    select * from {{ ref('stg_employees') }}

),

products as (
    
    select * from {{ ref('stg_products') }}

),

companies as (
    
    select * from {{ ref('stg_companies') }}

),

invoice_totals as(

    select * from {{ ref('int_invoice_totals') }}
),

invoice_detail as (

    select
    concat('INV-',{{ dbt_utils.generate_surrogate_key(['order_date','emp_id']) }}) as invoice_id,
    emp_id,
    order_id,
    comp_id,
    total_items as lines_count,
    total_gross_value as invoice_gross,
    round(total_gross_value + emp_disc_value,2) as invoice_net_before_vat,
    --env.disc_applied,
    --env.emp_disc_value,
    total_vat as invoice_vat_total,
    total_net_value as invoice_total
    from invoice_totals
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
    inv.invoice_gross,
    inv.invoice_net_before_vat,
    inv.invoice_vat_total,
    inv.invoice_total,
    inv.invoice_value_bucket,
    current_timestamp() as generated_at
    from invoice_class inv
    left join companies c on inv.comp_id = c.comp_id
    left join employees e on inv.emp_id = e.emp_id
    order by record_type, invoice_total desc nulls last, generated_at desc

)

-- Simple SELECT statement.

select * from invoice_final