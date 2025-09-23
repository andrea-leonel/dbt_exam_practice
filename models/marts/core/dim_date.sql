with spine as (

{{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2020-01-01' as date)",
    end_date="cast('2026-01-01' as date)"
   )
}}

),

expanded as (

    select 
    cast(date_day as date) as date,
    extract(day from date_day) as day_num,
    format_date('%A', date_day) as week_day,
    extract(isoweek from date_day) as week_num,
    extract(month from date_day) as month_num,
    format_date('%B', date_day) as month,
    extract(quarter from date_day) as quarter_num,
    extract(year from date_day) as year
    from spine

)


select * from expanded
