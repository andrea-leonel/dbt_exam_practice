with companies as (

    select * from {{ ref('stg_companies') }}
),

orders as (

    select * from {{ ref('stg_orders') }}

),

orders_with_company as (

    select a.*, b.comp_id from orders a
    left join {{ ref('stg_employees') }} b on a.emp_id = b.emp_id 
),

orders_with_product as (

    select a.*, b.category, b.price, b.prod_name from orders_with_company a
    left join {{ ref('stg_products') }} b on a.prod_id = b.prod_id
),

product_detail as (

    select
    a.comp_id,
    b.prod_id,
    b.prod_name,
    count(distinct b.order_id) as times_ordered
    from companies a
    left join orders_with_product b on a.comp_id = b.comp_id
    group by a.comp_id, b.prod_id, b.prod_name

),

ranked_product_detail as (

    select
    comp_id,
    prod_id,
    prod_name,
    times_ordered,
    row_number() over (
        partition by comp_id
        order by times_ordered desc
    ) as times_ranked
    from product_detail

),

product_detail_final as (

    select 
    a.*,
    b.prod_name as prod_most_ordered
    from companies a
    left join ranked_product_detail b on a.comp_id = b.comp_id
    where b.times_ranked = 1
)

select * from product_detail_final

