{% macro drop_model(model_name, model_type) %}
    {% set sql %}
        use role dbt_funcrole;
        drop {{ model_type }} if exists {{ model_name }};
    {% endset %}
    {% do run_query(sql) %}
    {% do dbt_utils.log_info("Successfully deleted " ~ model_type ~ " " ~ model_name) %}
{% endmacro %}
