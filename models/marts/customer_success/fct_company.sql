with companies as (

    select * from {{ ref('int_company_detail') }}
),

aggregations as (

    select 
    a.comp_id,
    a.prod_most_ordered,
    count(distinct b.emp_id) as num_emp,
    count(distinct b.order_id) as num_orders,
    max(b.order_date) as latest_order_date,
    min(b.order_date) as first_order_date,
    max(b.total_gross_value) as max_order_gross,
    min(b.total_gross_value) as min_order_gross,
    round(avg(b.total_gross_value),2) as avg_order_gross,
    max(b.total_num_items) as max_order_basket,
    min(b.total_num_items) as min_order_basket
    from companies a
    left join {{ ref('int_order_totals') }} b on a.comp_id = b.comp_id
    group by a.comp_id, a.prod_most_ordered

),



join_all as (

    select 
    comp_id,
    num_emp,
    num_orders,
    case
        when num_orders = 0 then 'Never ordered'
        when num_orders = 1 then 'Single order'
        when num_orders > 1 then 'Multiple orders' else null
    end as status,
    latest_order_date,
    first_order_date,
    max_order_gross,
    min_order_gross,
    avg_order_gross,
    max_order_basket,
    min_order_basket,
    prod_most_ordered
    from aggregations
)

select * from join_all
