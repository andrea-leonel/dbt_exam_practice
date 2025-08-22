with companies_base as  (

    select * from {{ source('dbt_fake','companies_base') }}

),

renamed as (

    select 
    id as company_id,
    name as company_name,
    slogan as company_slogan,
    purpose as company_purpose,
    date_added as company_date_added
    from companies_base

),

remove_nulls as (

    select * from renamed
    where company_id is not null

)

select * from remove_nulls

