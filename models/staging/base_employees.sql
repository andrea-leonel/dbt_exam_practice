with employees_base as  (

    select * from {{ source('dbt_fake','employees_base') }}
    where id is not null

),

employees_additional as  (

    select 
    id as id_additional,
    phone_number,
    address, 
    birthdate,
    blood_type,
    favorite_color,
    credit_score
    from {{ source('dbt_fake_2','fake_personal_info') }}
    where id is not null

),

employees_join as (

    select * from employees_base a
    left join employees_additional b on a.id = b.id_additional
)

select * from employees_join