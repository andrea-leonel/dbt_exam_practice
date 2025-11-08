with companies_base as  (

    select * from {{ source('dbt_fake','companies_base') }}

),

renamed as (

    select 
    trim(safe_cast(id as string)) as comp_id,
    name as comp_name,
    slogan,
    purpose,
    date_added as comp_dateadded
    from companies_base

),

remove_nulls as (

    select * from renamed
    where comp_id is not null

)

select * from remove_nulls

