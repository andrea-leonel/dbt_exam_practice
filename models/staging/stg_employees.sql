with employees_base as  (

    select * from {{ ref('base_employees') }}

),

reworked as (

    select 
    id as emp_id,
    company_id as comp_id,
    first_name, 
    last_name,
    concat(first_name,' ',last_name) as full_name,
    case 
        when gender = 'female' then 'F'
        when gender = 'male' then 'M'
        else null 
    end as gender,
    email as email,
    date_diff(current_date,birthdate, year) as age,
    username as username,
    date_added as emp_date_added,
    phone_number as phone_number,
    json_extract_scalar(address, '$.street_address') as address_street,
    json_extract_scalar(address, '$.city') as address_city,
    json_extract_scalar(address, '$.state') as address_state,
    json_extract_scalar(address, '$.zipcode') as address_zipcode,
    birthdate,
    blood_type,
    favorite_color,
    credit_score
    from employees_base

)

select * from reworked