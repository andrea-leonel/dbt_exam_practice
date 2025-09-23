with products as (

    select 
    *,
    date_diff(current_date(),prod_date_added, year) as prod_years_joined,
    from {{ ref('stg_products') }}

)

select * from products
