with
    raw_data_mwr as (
        select
            a.job_number,
            a.division,
            cast(nullif(a.date_received, 'null') as datetime) as date_received,
            cast(nullif(a.date_contacted, 'null') as datetime) as date_contacted,
            cast(nullif(a.date_inspected, 'null') as datetime) as date_inspected,
            cast(nullif(a.date_estimate_sent, 'null') as datetime) as date_estimate_sent,
            cast(nullif(a.date_started, 'null') as datetime) as date_started,
            cast(nullif(a.date_closed, 'null') as datetime) as date_closed,
            cast(
                nullif(replace(replace(a.total_invoiced, '$', ''), ',', ''), 'null') as float64
            ) as total_invoiced,
            cast(
                nullif(replace(replace(a.total_job_cost, '$', ''), ',', ''), 'null') as float64
            ) as total_job_cost,
            cast(
                nullif(replace(replace(a.working_gp, '$', ''), ',', ''), 'null') as float64
            ) as working_gp_pct,
            cast(nullif(a.estimator_name, 'null') as string) as estimator,
            cast(
                nullif(a.date_of_work_authorization, 'null') as datetime
            ) as date_work_authorization,
        from {{ source("mail_exports", "fedex_kpi_revive") }} a
    ),
    data_flagged as (
        select
            'Revive' as source,
            job_number,
            coalesce(date_closed, date_started) as date_received,
            total_invoiced,
            total_job_cost,
            working_gp_pct,
            date_diff(
                coalesce(
                    date_closed, date_started, date_estimate_sent, date_inspected, date_contacted
                ),
                date_received,
                day
            ) as consecutive_days,
            if(
                date_started is not null or date_work_authorization is not null, true, false
            ) as is_win,
            if(
                date_started is null
                and date_work_authorization is null
                and date_closed is not null,
                true,
                false
            ) as is_loss,
            if(
                (date_started is null and date_work_authorization is null and date_closed is null),
                true,
                false
            ) as is_pending,
            if(
                (
                    (date_started is not null or date_work_authorization is not null)
                    and date_closed is null
                ),
                true,
                false
            ) as is_inprogress,
            if(
                (
                    date_estimate_sent is null
                    or date_inspected is null
                    or date_contacted is null
                    or date_received is null
                )
                and (date_started is not null or date_work_authorization is not null),
                true,
                false
            ) as is_incomplete,
            if(
                date_received is not null and date_contacted is not null, true, false
            ) as lead_to_contact,
            if(
                date_received is not null and date_inspected is not null, true, false
            ) as lead_to_inspect
        from raw_data_mwr
    ),
    data_loaded as (
        select
            source,
            datetime_trunc(date_received, month) as week_date,
            count(distinct job_number) as jobs,
            count(distinct if(is_win, job_number, null)) as wins,
            sum(if(is_win, total_invoiced, 0)) as total_invoiced,
            avg(
                if(is_win, if(total_invoiced > 0, total_invoiced, null), null)
            ) as avg_total_invoiced
        from data_flagged
        group by source, datetime_trunc(date_received, month)
    )
select *
from data_loaded
union all
select
    source,
    datetime_trunc(current_date(), month) as week_date,
    0 as jobs,
    0 as wins,
    0 as total_invoiced,
    0 as avg_total_invoiced
from data_loaded
where
    week_date = (select max(week_date) from data_loaded)
    and extract(year from current_date()) + extract(month from current_date()) not in (
        select distinct extract(year from week_date) + extract(month from week_date)
        from data_loaded
    )
