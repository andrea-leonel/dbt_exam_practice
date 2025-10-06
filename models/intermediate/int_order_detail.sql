with orders as (

    select * from {{ ref('stg_orders') }}

),

add_company as (

    select a.*, b.comp_id from orders a
    left join {{ ref('stg_employees') }} b on a.emp_id = b.emp_id 
),

add_product as (

    select a.*, b.category, b.price, b.prod_name from add_company a
    left join {{ ref('stg_products') }} b on a.prod_id = b.prod_id
),

order_value as (

    select
        order_id,
        order_date,
        emp_id,
        comp_id,
        prod_id,
        prod_name,
        category,
        price,
        num_items,
        round(safe_multiply(num_items,price),2) as product_value
    from add_product

)

select * from order_value
