with products_base as (

    select * from {{ source('dbt_fake','products_base') }}
),

manufacturing_status as (

    select 
    trim(safe_cast(product_id as string)) as prod_id,
    manufacturing_status
    from {{ ref('prod_manufacturing_status') }}
),

removed_null as (

    select * from products_base
    where id is not null

),

renamed as (
    
    select
        trim(safe_cast(id as string)) as prod_id,
        category,
        name as prod_name,
        price,
        date_added as prod_date_added
        from removed_null

),

join_manuf_status as (

    select a.*,
    b.manufacturing_status
    from renamed a
    left join manufacturing_status b on a.prod_id = b.prod_id

)


select * from join_manuf_status

