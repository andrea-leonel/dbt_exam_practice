with tenure as (

    select 
    *,
    date_diff(current_date(),comp_dateadded, year) as comp_years_joined
    from {{ ref('stg_companies') }}

)

select * from tenure

