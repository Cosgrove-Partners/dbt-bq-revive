{% macro update_base_fedex_kpi() %}
    {% do run_query("call {{schema}}.sp_upd_base_fedex_kpi();") %}
{% endmacro %}
