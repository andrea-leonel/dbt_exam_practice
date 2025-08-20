with orders as  (

    select * from {{ source('dbt_fake','enterprise_orders_base') }}

)

select * from orders