{% macro get_columns_for_model(model_name) %}
    {% set relation = ref(model_name) %}
    {% set columns = adapter.get_columns_in_relation(relation) %}
    {% for col in columns %}
        {{ log(col.name, info=True) }}
    {% endfor %}
{% endmacro %}
