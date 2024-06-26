{% macro add_metadata() %}
    {% set row_exists %}
        select count(*)
        from logging.events
        where dataset = '{{this.schema}}'
          and `table` = '{{this.name}}'
    {% endset %}

    {% if run_query(row_exists) == 0 %}
        insert into logging.events values ('{{this.schema}}','{{this.name}}',current_timestamp, (select count(*) from {{this}}),0)
    {% else %}
        update logging.events
        set `rows` = (select count(*) from {{this}}),
            last_build = current_timestamp,
            diff_last_run = `rows` - (select count(*) from {{this}})
        where dataset = '{{this.schema}}'
          and `table` = '{{this.name}}'
    {% endif %}
{% endmacro %}
