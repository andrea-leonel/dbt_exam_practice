with orders as  (

    select * from {{ source('dbt_fake','enterprise_orders_base') }}

),

renamed as (

    select 
    date as order_date,
    employee_id,
    product_id,
    num_items
    from orders
),

order_id_add as (

    select 
    GENERATE_UUID() as order_id,
    *
    from renamed
)

select * from order_id_add