{% macro vat_category(category) %}

case
    when lower({{ category }}) like '%food%' then 0.05
    when lower({{ category }}) like '%electronics%' then 0.20
    when lower({{ category }}) like '%clothing%' then 0.12
    else 0.18
end

{% endmacro %}