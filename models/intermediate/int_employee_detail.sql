with employees as (

    select 
    *,
    date_diff(current_date(),emp_date_added, year) as emp_years_joined,
    from {{ ref('stg_employees') }}

),

orders as (

    select * from {{ ref('stg_orders') }}

),

orders_with_product as (

    select a.*, b.category, b.price, b.prod_name from orders a
    left join {{ ref('stg_products') }} b on a.prod_id = b.prod_id
),

product_detail as (

    select
    a.emp_id,
    b.prod_id,
    b.prod_name,
    count(distinct b.order_id) as times_ordered
    from employees a
    left join orders_with_product b on a.emp_id = b.emp_id
    group by a.emp_id, b.prod_id, b.prod_name

),

ranked_product_detail as (

    select
    emp_id,
    prod_id,
    prod_name,
    times_ordered,
    row_number() over (
        partition by emp_id
        order by times_ordered desc
    ) as times_ranked
    from product_detail

),

product_detail_final as (

    select 
    a.emp_id,
    a.comp_id,
    b.prod_name as prod_most_ordered
    from employees A
    left join ranked_product_detail b on a.emp_id = b.emp_id
    where b.times_ranked = 1
)

select * from product_detail_final

