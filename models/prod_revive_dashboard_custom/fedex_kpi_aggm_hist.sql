{{
    config(
        materialized="incremental",
        unique_key=["source", "forecast_mode", "day_forecast"],
        tags=["incremental_model"],
    )
}}

select
    source,
    forecast_mode,
    date_trunc(current_date(), day) as day_forecast,
    jobs as leads,
    wins as starts,
    wins_to_date as estimated_wins
from {{ ref("fedex_kpi_aggm_tmp") }}

{% if is_incremental() %} where 1 = 1 {% endif %}
