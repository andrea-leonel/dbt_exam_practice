with products_base as (

    select * from {{ source('dbt_fake','products_base') }}
),

removed_null as (

    select * from products_base
    where id is not null

),

renamed as (
    
    select
        id as prod_id,
        category,
        name as prod_name,
        price,
        date_added as prod_date_added
        from removed_null

)

select * from renamed

