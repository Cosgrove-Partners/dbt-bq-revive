{{ config(materialized="table", post_hook="call {{schema}}.sp_upd_base_fedex_kpi();") }}

with
    original_data as (select *, 'Revive' as branch from {{ ref("src_fedex_kpi") }}),
    std_calc_fields as (
        select
            *,
            date_diff(date_contacted, date_received, day) as days_to_contact,
            date_diff(date_inspected, date_contacted, day) as days_to_inspect,
            date_diff(date_estimate_sent, date_inspected, day) as days_to_send_estimate,
            date_diff(date_of_work_authorization, date_estimate_sent, day) as days_to_authorize,
            date_diff(date_started, date_of_work_authorization, day) as days_to_start,
            date_diff(
                date_of_majority_completion, date_started, day
            ) as days_to_majority_completion,
            date_diff(referral_fee_date_paid, date_of_majority_completion, day) as days_to_paid_fee,
            date_diff(date_invoiced, referral_fee_date_paid, day) as days_to_invoice,
            date_diff(date_closed, date_invoiced, day) as days_to_close,
            date_diff(
                coalesce(
                    date_closed,
                    date_invoiced,
                    referral_fee_date_paid,
                    date_of_majority_completion,
                    date_started,
                    date_of_work_authorization,
                    date_estimate_sent,
                    date_inspected,
                    date_contacted
                ),
                date_received,
                day
            ) as consecutive_days,
            date_diff(date_contacted, date_received, hour) as hours_to_contact,
            date_diff(date_inspected, date_contacted, hour) as hours_to_inspect,
            date_diff(date_estimate_sent, date_inspected, hour) as hours_to_send_estimate,
            date_diff(date_of_work_authorization, date_estimate_sent, hour) as hours_to_authorize,
            date_diff(date_started, date_of_work_authorization, hour) as hours_to_start,
            date_diff(
                date_of_majority_completion, date_started, hour
            ) as hours_to_majority_completion,
            date_diff(
                referral_fee_date_paid, date_of_majority_completion, hour
            ) as hours_to_paid_fee,
            date_diff(date_invoiced, referral_fee_date_paid, hour) as hours_to_invoice,
            date_diff(date_closed, date_invoiced, hour) as hours_to_close,
            date_diff(
                coalesce(
                    date_closed,
                    date_invoiced,
                    referral_fee_date_paid,
                    date_of_majority_completion,
                    date_started,
                    date_of_work_authorization,
                    date_estimate_sent,
                    date_inspected,
                    date_contacted
                ),
                date_received,
                hour
            ) as consecutive_hours,
            if(
                date_started is not null or date_of_work_authorization is not null, true, false
            ) as is_win,
            if(date_started is null and date_closed is not null, true, false) as is_loss,
            if(
                (
                    date_started is null
                    and date_of_work_authorization is null
                    and date_closed is null
                ),
                true,
                false
            ) as is_pending,
            if((date_started is not null and date_closed is null), true, false) as is_inprogress,
            if(
                (
                    date_estimate_sent is null
                    or date_inspected is null
                    or date_contacted is null
                    or date_received is null
                )
                and date_started is not null,
                true,
                false
            ) as is_incomplete,
            if(
                date_received is not null and date_contacted is not null, true, false
            ) as lead_to_contact,
            if(
                date_received is not null and date_inspected is not null, true, false
            ) as lead_to_inspect,
            if(
                division = 'reconstruction',
                if(job_completion_percentage = 55, true, false),
                if(starts_with(secondary_loss_type, 'bad'), true, false)
            ) as bad_lead,
            datetime_trunc(date_received, week) as week_date_received,
            datetime_trunc(date_started, week) as week_date_started,
            datetime_trunc(date_received, month) as month_date_received,
            datetime_trunc(date_started, month) as month_date_started,
            extract(year from date_received) as year_date_received,
            extract(year from date_started) as year_date_started,
            if(
                date_started is not null, if(total_invoiced > 0, total_invoiced, null), null
            ) as win_total_invoiced_norm,
            if(total_invoiced > 0, total_invoiced, null) as total_invoiced_norm,
            if(
                date_closed is null
                or date_invoiced is null
                or referral_fee_date_paid is null
                or date_of_majority_completion is null
                or date_started is null
                or date_of_work_authorization is null
                or date_estimate_sent is null
                or date_inspected is null
                or date_contacted is null,
                true,
                false
            ) as data_issue
        from original_data
    )
select
    *,
    percentile_cont(win_total_invoiced_norm, 0.5) over (
        partition by branch, week_date_received
    ) as week_win_med_total_invoiced,
    percentile_cont(win_total_invoiced_norm, 0.5) over (
        partition by branch, month_date_received
    ) as month_win_med_total_invoiced,
    percentile_cont(total_invoiced_norm, 0.5) over (
        partition by branch, week_date_received
    ) as week_med_total_invoiced,
    percentile_cont(total_invoiced_norm, 0.5) over (
        partition by branch, month_date_received
    ) as month_med_total_invoiced,
    avg(win_total_invoiced_norm) over (
        partition by branch, week_date_received
    ) as week_win_avg_total_invoiced,
    avg(win_total_invoiced_norm) over (
        partition by branch, month_date_received
    ) as month_win_avg_total_invoiced,
    avg(total_invoiced_norm) over (
        partition by branch, week_date_received
    ) as week_avg_total_invoiced,
    avg(total_invoiced_norm) over (
        partition by branch, month_date_received
    ) as month_avg_total_invoiced
from std_calc_fields
where year_date_received >= (extract(year from current_date()) - 5)
