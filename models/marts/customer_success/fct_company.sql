with companies as (

    select * from {{ ref('stg_companies') }}
),

aggregations as (

    select 
    a.comp_id,
    count(distinct b.emp_id) as num_emp,
    count(distinct b.order_id) as num_orders,
    max(b.order_date) as latest_order_date,
    min(b.order_date) as first_order_date,
    max(b.order_value) as max_order_value,
    min(b.order_value) as min_order_value,
    round(avg(b.order_value),2) as avg_order_value,
    max(b.num_items) as max_order_basket,
    min(b.num_items) as min_order_basket
    from companies a
    left join {{ ref('int_order_detail') }} b on a.comp_id = b.comp_id
    group by a.comp_id

),

product_detail as (

    select
    a.comp_id,
    b.prod_id,
    b.prod_name,
    count(distinct b.order_id) as times_ordered
    from aggregations a
    left join {{ ref('int_order_detail') }} b on a.comp_id = b.comp_id
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
    a.comp_id,
    b.prod_name
    from companies A
    left join ranked_product_detail b on a.comp_id = b.comp_id
    where b.times_ranked = 1
),

join_all as (

    select 
    a.comp_id,
    b.num_emp,
    b.num_orders,
    case
        when b.num_orders = 0 then 'Never ordered'
        when b.num_orders = 1 then 'Single order'
        when b.num_orders > 1 then 'Multiple orders' else null
    end as status,
    b.latest_order_date,
    b.first_order_date,
    b.max_order_value,
    b.min_order_value,
    b.avg_order_value,
    b.max_order_basket,
    b.min_order_basket,
    c.prod_name as prod_most_ordered
    from companies a
    left join aggregations b on a.comp_id = b.comp_id
    left join product_detail_final c on a.comp_id = c.comp_id
)

select * from join_all
