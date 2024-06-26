{{
    config(
        materialized="incremental",
        unique_key=["source", "forecast_mode", "week_date"],
        tags=["incremental_model"],
    )
}}

select
    s.source,
    s.week_date,
    s.jobs,
    s.wins,
    s.total_invoiced,
    s.avg_total_invoiced,
    s.win_pct,
    s.best_month,
    s.last_month,
    s.new_month,
    s.days_in_month,
    s.days_to_date,
    s.leads_per_day,
    s.forecast_mode,
    s.previous_win_pct,
    s.estimated_wins_per_day,
    s.wins_in_month,
    s.wins_to_date,
    s.avg_total_invoiced_ytd,
    s.total_invoiced_in_month
from {{ ref("fedex_kpi_aggm_tmp") }} s

{% if is_incremental() %}
    where s.week_date >= date_sub(current_date(), interval 12 month)
{% endif %}
