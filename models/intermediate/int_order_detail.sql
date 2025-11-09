with orders as (

    select * from {{ ref('stg_orders') }}

),

add_company as (

    select a.*, b.comp_id, b.state_code as emp_state from orders a
    left join {{ ref('stg_employees') }} b on a.emp_id = b.emp_id 
),

company_detail as (

    select 
    e.*,
    c.comp_dateadded
    from add_company e
    left join {{ ref('stg_companies') }} c on c.comp_id = e.comp_id

),

add_product as (

    select a.*, b.category, b.price, b.prod_name, b.manufacturing_status from company_detail a
    left join {{ ref('stg_products') }} b on a.prod_id = b.prod_id
),

discount_prod as (

    select
    *,
    {{ vat_category('category') }} as vat_perc
    from add_product

),

order_value as (

    select
        *,
        round(safe_multiply(num_items,price),2) as product_value
    from discount_prod

),

order_vat as (

    select
    *,
    round(safe_multiply(product_value,vat_perc),2) as vat_value
    from order_value

)

select * from order_vat
