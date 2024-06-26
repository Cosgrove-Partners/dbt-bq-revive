with
    raw_leads as (
        select
            division,
            job_number,
            max(date_started) as date_started,
            max(date_received) as date_received,
            sum(total_invoiced) as total_invoiced,
            max(total_job_cost) as total_job_cost,
            max(date_inspected) as date_inspected
        from {{ ref("stg_fedex_kpi") }}
        group by division, job_number
    )
select a.*, datetime_trunc(date_started, month) as month_date_started
from raw_leads a
