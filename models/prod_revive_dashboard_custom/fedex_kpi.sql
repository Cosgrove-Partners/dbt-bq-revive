with
    inittable as (
        select
            job_number,
            datetime_trunc(date_received, week) as week_date,
            division,
            estimator,
            is_win,
            is_loss,
            is_pending,
            is_inprogress,
            is_incomplete,
            ifnull(days_to_contact, -99999) as contact,
            ifnull(days_to_inspect, -99999) as inspect,
            ifnull(days_to_send_estimate, -99999) as estimate,
            ifnull(days_to_start, -99999) as start,
            ifnull(days_to_close, -99999) as finish,
            if(date_contacted is null, 1, 0) as date_contacted_empty,
            if(date_inspected is null, 1, 0) as date_inspected_empty,
            if(date_estimate_sent is null, 1, 0) as date_estimated_empty,
            if(date_started is null, 1, 0) as date_started_empty,
            if(date_closed is null, 1, 0) as date_closed_empty,
            total_invoiced,
            consecutive_days,
            lead_to_contact,
            lead_to_inspect
        from {{ ref("stg_fedex_kpi") }}
    ),
    pivottable as (
        select *
        from inittable unpivot (days for phase in (contact, inspect, estimate, start, finish))
    )
select
    job_number,
    week_date,
    division,
    estimator,
    is_win,
    is_loss,
    is_pending,
    is_inprogress,
    is_incomplete,
    lead_to_contact,
    lead_to_inspect,
    phase,
    if(days = -99999, null, days) as days,
    total_invoiced,
    consecutive_days,
    case
        when phase = 'Contact'
        then date_contacted_empty
        when phase = 'Inspect'
        then date_inspected_empty
        when phase = 'Estimate'
        then date_estimated_empty
        when phase = 'Start'
        then date_started_empty
        when phase = 'Finish'
        then date_closed_empty
    end as empty_phase
from pivottable
