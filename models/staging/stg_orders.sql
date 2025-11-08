with orders as  (

    select * from {{ source('dbt_fake','enterprise_orders_base') }}

),

reworked as (

    select 
    {{ dbt_utils.generate_surrogate_key(['date', 'employee_id']) }} as order_id,
    date as order_date,
    trim(safe_cast(employee_id as string)) as emp_id,
    trim(safe_cast(product_id as string)) as prod_id,
    num_items
    from orders
)

select * from reworked