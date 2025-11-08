with employees_base as  (

    select * from {{ ref('base_employees') }}

),

state_codes as (

    select * from {{ ref('us_states') }}
),

area_codes as (

    select 
    trim(safe_cast(country_code as string)) as country_code,
    country_name
    from {{ ref('international_area_codes') }}
),

reworked as (

    select 
    trim(safe_cast(id as string)) as emp_id,
    trim(safe_cast(company_id as string)) as comp_id,
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

),

add_state_code as (

    select a.*,
    b.state_code,
    from reworked a
    left join state_codes b on a.address_state = b.state_name
),

add_area_code as (

    select a.*,
    b.country_name as phone_country
    from add_state_code a 
    left join area_codes b 
    on trim(substr(a.phone_number,strpos(a.phone_number,'+')+1,(strpos(a.phone_number,' - ')-strpos(a.phone_number,'+')))) = b.country_code
)

select * from add_area_code