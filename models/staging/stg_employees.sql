with employees_base as  (

    select * from {{ ref('base_employees') }}

),

renamed as (

    select 
    id as employee_id,
    first_name as employee_first_name, 
    last_name as employee_last_name,
    gender as employee_gender,
    email as employee_email,
    date_diff(current_date,birthdate, year) as employee_age,
    username as employee_username,
    date_added as employee_date_added,
    company_id,
    phone_number as employee_phone_number,
    address as employee_address,
    birthdate as employee_birthdate,
    blood_type as employee_blood_type,
    favorite_color as employee_favorite_color,
    credit_score as employee_credit_score
    from employees_base

),

gender_abbv as (

    select 
    employee_id,
    case 
        when employee_gender = 'female' then 'F'
        when employee_gender = 'male' then 'M' else null
    end as employee_gender
    from renamed
),

new_gender as (

    select 
    a.employee_id,
    a.company_id,
    a.employee_date_added,
    a.employee_first_name, 
    a.employee_last_name,
    b.employee_gender,
    a.employee_age,
    a.employee_birthdate,
    a.employee_email,
    a.employee_username,
    a.employee_phone_number,
    a.employee_address,
    a.employee_blood_type,
    a.employee_favorite_color,
    a.employee_credit_score
    from renamed a
    left join gender_abbv b on a.employee_id= b.employee_id
)

select * from new_gender