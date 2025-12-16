{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    
    {%- if custom_schema_name is none -%}
        {# 如果沒有特別設定 schema，就用 profiles.yml 裡的預設值 #}
        {{ default_schema }}

    {%- else -%}
        {# 如果有設定 schema (例如 intermediate)，就直接使用該名稱，不要加前綴 #}
        {# 這裡我們加上 'olist_' 前綴，以配合你在 Terraform 建立的 dataset 名稱 #}
        olist_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}