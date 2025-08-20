with employees_base as  (

    select * from {{ source('dbt_fake','employees_base') }}
    where id is not null

),

employees_additional as  (

    select * from {{ source('dbt_fake_2','fake_personal_info') }}
    where id is not null

),

employees_join as (

    select * from employees_base a
    left join employees_additional b on a.id = b.id
)

select * from employees_join