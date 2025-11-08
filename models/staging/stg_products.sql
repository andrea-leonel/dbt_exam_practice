with products_base as (

    select * from {{ source('dbt_fake','products_base') }}
),

manufacturing_status as (

    select * from {{ ref('prod_manufacuring_status') }}
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

),

join_manuf_status as (

    select a.*,
    b.manufacturing_status
    from renamed a
    left join manufacturing_status b on safe_cast(a.prod_id as string) = safe_cast(b.prod_id as string)

)

select * from join_manuf_status

