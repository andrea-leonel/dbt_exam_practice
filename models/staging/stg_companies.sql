with companies_base as  (

    select * from {{ source('dbt_fake','companies_base') }}

)

select * from companies_base