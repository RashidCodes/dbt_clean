{% macro extract_information_schema(table_catalog) %}
    {% set sql %}
        select * from {{ table_catalog }}.information_schema.tables where lower(table_owner) = 'dbt_funcrole';
    {% endset %}
    {% do run_query(sql) %}
{% endmacro %}
