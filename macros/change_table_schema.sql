
{% macro add_fields(schema, table, middle_fields={}, last_fields={} , database=target.database) %}
    {% set schema = target.schema ~ '_' ~ schema %} 
    {% set table_full_name = schema ~ '.' ~ table%}

    {{ log('Adding fields to ' ~ table_full_name, info=True) }}

    {% set table_relation = adapter.get_relation(
        database=database,
        schema=schema,
        identifier=table
    ) %}

    {% set columns = adapter.get_columns_in_relation(table_relation) %}
    
    {%set create_replace%}
        CREATE OR REPLACE TABLE `{{table_full_name}}` AS
        SELECT
            {# Add existing fields and new middle fields#}
            {% for column in columns %}
                {% set new_fields = middle_fields.get(column.name) %}
                {% if new_fields %}
                    {% for field in new_fields %}
                        CAST(NULL AS {{field['type']}}) AS {{field['name'] }},
                    {% endfor %}
                {% endif %}
                
                {{column.name}}
                
                {% if not loop.last %},{% endif %}
            {% endfor %}

            {# Append last fields #}
            {% if last_fields %}
              ,
            {% endif %}

            {% for name, type in last_fields.items() %}
                CAST(NULL AS {{type}}) AS {{name}}

                {% if not loop.last %},{% endif %} 
            {% endfor %}
        FROM
           `{{table_full_name}}`
    {%endset%}
    {% do run_query(create_replace)%}
{% endmacro %}

{% macro remove_fields(schema, table, fields, database=target.database) %}
    {% set schema = target.schema ~ '_' ~ schema %} 
    {% set table_full_name = schema ~ '.' ~ table%}
    {% set fields_copy = fields.copy() %}

    {{ log('Removing fields ' ~ fields ~ ' from ' ~ table_full_name, info=True) }}

    {% set table_relation = adapter.get_relation(
        database=database,
        schema=schema,
        identifier=table
    ) %}

    {% set columns = adapter.get_columns_in_relation(table_relation) %}
    
    {%set create_replace%}
        CREATE OR REPLACE TABLE `{{table_full_name}}` AS
        SELECT
            {% for column in columns %}
                {% if column.name in fields_copy %}
                    {% do fields_copy.remove(column.name) %}
                {% else %}
                    {{column.name}}
                    {% if not loop.last %},{% endif %}
                {% endif %}
            {% endfor %}
        FROM
           `{{table_full_name}}`
    {%endset%}
    {% do run_query(create_replace)%}
{% endmacro %}

{% macro rename_fields(schema, table, fields, database=target.database) %}
    {% set schema = target.schema ~ '_' ~ schema %} 
    {% set table_full_name = schema ~ '.' ~ table%}

    {{ log('Renaming fields in ' ~ table_full_name, info=True) }}

    {% set table_relation = adapter.get_relation(
        database=database,
        schema=schema,
        identifier=table
    ) %}

    {% set columns = adapter.get_columns_in_relation(table_relation) %}
    
    {%set create_replace%}
        CREATE OR REPLACE TABLE `{{table_full_name}}` AS
        SELECT
            {% for column in columns %}
                {% set new_field = fields.get(column.name) %}
                {% if new_field %}
                   {{column.name}} AS {{new_field}}  
                {% else %}
                    {{column.name}}
                {% endif %}
                    {% if not loop.last %},{% endif %}
            {% endfor %}
        FROM
           `{{table_full_name}}`
    {%endset%}
    {% do run_query(create_replace)%}
{% endmacro %}
