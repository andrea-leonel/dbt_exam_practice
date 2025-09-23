with orders as (

    select * from {{ ref('int_order_detail') }}

)

select * from orders
