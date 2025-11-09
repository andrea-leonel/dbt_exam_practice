{% macro disc_loyalty(comp_dateadded, company_orders) %}

case
    when date_diff(current_date(), {{ comp_dateadded }}, year)>= 5 then 0.10
    when date_diff(current_date(), {{ comp_dateadded }}, year)>= 2 then 0.05
    when {{ company_orders }} > 200 then 0.04
    else 0.00 
end   

{% endmacro %}

{% macro disc_volume(employee_orders) %}

case
    when {{ employee_orders }} >= 100 then 0.05
    when {{ employee_orders }} >= 50 then 0.03
    when {{ employee_orders }} >= 10 then 0.01
    else 0.00
end
  

{% endmacro %}