with orders as (
    select * from {{ ref('int_order_detail') }}
),

order_value as (
    select 
    *,
    product_value + vat_value as order_value
    from orders

)
select * from order_value
