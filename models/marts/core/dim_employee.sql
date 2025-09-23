with employees as (

    select 
    *,
    date_diff(current_date(),emp_date_added, year) as emp_years_joined,
    from {{ ref('stg_employees') }}

)

select * from employees