with
    raw_data as (
        select
            job_number,
            division,
            date_received,
            date_contacted,
            date_inspected,
            date_estimate_sent,
            date_started,
            date_closed,
            total_invoiced,
            total_job_cost,
            working_gp_pct,
            estimator,
            secondary_loss_type,
            job_completion_percentage,
            hours_to_contact as days_to_contact,
            hours_to_inspect as days_to_inspect,
            hours_to_send_estimate as days_to_send_estimate,
            hours_to_start as days_to_start,
            hours_to_close as days_to_close,
            consecutive_hours as consecutive_days,
            is_pending,
            is_inprogress,
            is_incomplete,
            lead_to_contact,
            lead_to_inspect,
            bad_lead,
            date_of_work_authorization
        from {{ ref("base_fedex_kpi") }}
    )
select
    *,
    if(date_started is not null or date_of_work_authorization is not null, true, false) as is_win,
    if(
        (date_started is null or date_of_work_authorization is null) and date_closed is not null,
        true,
        false
    ) as is_loss,
    'Report' as source
from raw_data
