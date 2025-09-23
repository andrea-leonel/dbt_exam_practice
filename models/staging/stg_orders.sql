with orders as  (

    select * from {{ source('dbt_fake','enterprise_orders_base') }}

),

reworked as (

    select 
    {{ dbt_utils.generate_surrogate_key(['date', 'employee_id']) }} as order_id,
    date as order_date,
    employee_id as emp_id,
    product_id as prod_id,
    num_items
    from orders
)

select * from reworked