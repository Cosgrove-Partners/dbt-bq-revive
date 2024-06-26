with
    raw_leads as (
        select
            datetime_trunc(date_received, month) as month_date,
            division,
            bad_lead,
            count(distinct job_number) as leads,
            count(distinct if(is_win, job_number, null)) as wins
        from {{ ref("stg_fedex_kpi") }}
        group by datetime_trunc(date_received, month), division, bad_lead
    ),
    pre_raw_starts as (
        select
            division,
            job_number,
            max(date_started) as date_started,
            sum(total_invoiced) as total_invoiced,
            max(total_job_cost) as total_job_cost,
            max(bad_lead) as bad_lead
        from {{ ref("stg_fedex_kpi") }}
        where is_win = true
        group by division, job_number
    ),
    raw_starts as (
        select
            datetime_trunc(date_started, month) as month_date,
            division,
            bad_lead,
            count(distinct job_number) as starts_in_month,
            sum(total_invoiced) as total_invoiced_in_month,
            sum(total_invoiced - total_job_cost) as gp_in_month,
        from pre_raw_starts
        group by datetime_trunc(date_started, month), division, bad_lead
    )
select a.*, b.starts_in_month, b.total_invoiced_in_month, b.gp_in_month
from raw_leads a
left join
    raw_starts b
    on a.month_date = b.month_date
    and a.division = b.division
    and a.bad_lead = b.bad_lead
