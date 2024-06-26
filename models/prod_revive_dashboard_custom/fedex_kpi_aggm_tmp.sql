with
    raw_data_mwr as (
        select
            *,
            wins / jobs as win_pct,
            rank() over (partition by source order by wins / jobs desc) as best_month,
            rank() over (partition by source order by week_date desc) - 1 as last_month,
            datetime_trunc(week_date, month) as new_month,
            extract(day from last_day(week_date)) as days_in_month,
            extract(day from current_date()) as days_to_date,
            jobs / extract(day from current_date()) as leads_per_day
        from {{ ref("stg_fedex_kpi_aggm") }}
        where week_date >= date_sub(current_date(), interval 12 month)
    ),
    mwr_data as (
        select
            *,
            'Last Month' as forecast_mode,
            (
                select win_pct
                from raw_data_mwr
                where last_month = 1 and source = a.source
            ) as previous_win_pct,
            leads_per_day * (
                select win_pct
                from raw_data_mwr
                where last_month = 1 and source = a.source
            ) as estimated_wins_per_day,
            (
                leads_per_day * (
                    select win_pct
                    from raw_data_mwr
                    where last_month = 1 and source = a.source
                )
            )
            * days_in_month as wins_in_month,
            (
                leads_per_day * (
                    select win_pct
                    from raw_data_mwr
                    where last_month = 1 and source = a.source
                )
            )
            * days_to_date as wins_to_date,
            (
                select avg(avg_total_invoiced)
                from raw_data_mwr
                where last_month = 1 and source = a.source
            ) as avg_total_invoiced_ytd,
            (
                (
                    leads_per_day * (
                        select win_pct
                        from raw_data_mwr
                        where last_month = 1 and source = a.source
                    )
                )
                * days_in_month
            ) * (
                select avg(avg_total_invoiced)
                from raw_data_mwr
                where last_month = 1 and source = a.source
            ) as total_invoiced_in_month
        from raw_data_mwr a
        where last_month = 0
        union all
        select
            *,
            'Best Month' as forecast_mode,
            (
                select win_pct
                from raw_data_mwr
                where best_month = 1 and source = a.source
            ) as previous_win_pct,
            leads_per_day * (
                select win_pct
                from raw_data_mwr
                where best_month = 1 and source = a.source
            ) as estimated_wins_per_day,
            (
                leads_per_day * (
                    select win_pct
                    from raw_data_mwr
                    where best_month = 1 and source = a.source
                )
            )
            * days_in_month as wins_in_month,
            (
                leads_per_day * (
                    select win_pct
                    from raw_data_mwr
                    where best_month = 1 and source = a.source
                )
            )
            * days_to_date as wins_to_date,
            (
                select avg(avg_total_invoiced)
                from raw_data_mwr
                where best_month = 1 and source = a.source
            ) as avg_total_invoiced_ytd,
            (
                (
                    leads_per_day * (
                        select win_pct
                        from raw_data_mwr
                        where best_month = 1 and source = a.source
                    )
                )
                * days_in_month
            ) * (
                select avg(avg_total_invoiced)
                from raw_data_mwr
                where best_month = 1 and source = a.source
            ) as total_invoiced_in_month
        from raw_data_mwr a
        where last_month = 0
        union all
        select
            *,
            'Trailing 3 Months' as forecast_mode,
            (
                select avg(win_pct)
                from raw_data_mwr
                where last_month between 1 and 3 and source = a.source
            ) as previous_win_pct,
            leads_per_day * (
                select avg(win_pct)
                from raw_data_mwr
                where last_month between 1 and 3 and source = a.source
            ) as estimated_wins_per_day,
            (
                leads_per_day * (
                    select avg(win_pct)
                    from raw_data_mwr
                    where last_month between 1 and 3 and source = a.source
                )
            )
            * days_in_month as wins_in_month,
            (
                leads_per_day * (
                    select avg(win_pct)
                    from raw_data_mwr
                    where last_month between 1 and 3 and source = a.source
                )
            )
            * days_to_date as wins_to_date,
            (
                select avg(avg_total_invoiced)
                from raw_data_mwr
                where last_month between 1 and 3 and source = a.source
            ) as avg_total_invoiced_ytd,
            (
                (
                    leads_per_day * (
                        select avg(win_pct)
                        from raw_data_mwr
                        where last_month between 1 and 3 and source = a.source
                    )
                )
                * days_in_month
            ) * (
                select avg(avg_total_invoiced)
                from raw_data_mwr
                where last_month between 1 and 3 and source = a.source
            ) as total_invoiced_in_month
        from raw_data_mwr a
        where last_month = 0
    )
select *
from mwr_data
