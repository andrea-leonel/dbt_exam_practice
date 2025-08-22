with products_base as (

    select * from {{ source('dbt_fake','products_base') }}
),

removed_null as (

    select * from products_base
    where id is not null

),

renamed as (
    
    select
        id as product_id,
        category as product_category,
        name as product_name,
        price as product_price,
        date_added as product_date_added
        from removed_null

)

select * from renamed

