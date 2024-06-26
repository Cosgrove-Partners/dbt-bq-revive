select
    job_number,
    division,
    cast(nullif(date_received, 'null') as datetime) as date_received,
    cast(nullif(date_contacted, 'null') as datetime) as date_contacted,
    cast(nullif(date_inspected, 'null') as datetime) as date_inspected,
    cast(nullif(date_estimate_sent, 'null') as datetime) as date_estimate_sent,
    cast(nullif(date_started, 'null') as datetime) as date_started,
    cast(nullif(date_closed, 'null') as datetime) as date_closed,
    cast(
        nullif(replace(replace(total_invoiced, '$', ''), ',', ''), 'null') as float64
    ) as total_invoiced,
    cast(
        nullif(replace(replace(total_job_cost, '$', ''), ',', ''), 'null') as float64
    ) as total_job_cost,
    cast(
        nullif(replace(replace(working_gp, '$', ''), ',', ''), 'null') as float64
    ) as working_gp_pct,
    cast(nullif(estimator_name, 'null') as string) as estimator,
    cast(
        nullif(replace(replace(total_estimates, '$', ''), ',', ''), 'null') as float64
    ) as total_estimates,
    loss_category,
    cast(nullif(date_estimate_approved, 'null') as datetime) as date_estimate_approved,
    cast(nullif(date_invoiced, 'null') as datetime) as date_invoiced,
    cast(nullif(date_of_cos, 'null') as datetime) as date_of_cos,
    cast(nullif(date_of_loss, 'null') as datetime) as date_of_loss,
    cast(nullif(date_of_majority_completion, 'null') as datetime) as date_of_majority_completion,
    cast(nullif(date_of_work_authorization, 'null') as datetime) as date_of_work_authorization,
    foreman,
    insurance_carrier,
    job_name,
    loss_address,
    loss_city as city,
    loss_state as state_province,
    type_of_loss,
    loss_zip as zip_postal_code,
    marketing_person,
    referred_by_marketing_campaign,
    referred_by_contact,
    referred_by,
    referred_by_company,
    referred_by_contact_category,
    referred_by_contact_type,
    cast(
        nullif(replace(replace(total_work_order_budget, '$', ''), ',', ''), 'null') as float64
    ) as total_work_order_budget,
    cast(
        nullif(replace(replace(subtrade_cost, '$', ''), ',', ''), 'null') as float64
    ) as subtrade_cost,
    cast(
        nullif(replace(replace(materials_cost, '$', ''), ',', ''), 'null') as float64
    ) as materials_cost,
    cast(nullif(replace(replace(other_cost, '$', ''), ',', ''), 'null') as float64) as other_cost,
    cast(
        nullif(replace(replace(referral_fee_cost, '$', ''), ',', ''), 'null') as float64
    ) as referral_fee_cost,
    cast(
        nullif(replace(replace(total_collected, '$', ''), ',', ''), 'null') as float64
    ) as total_collected,
    cast(nullif(lien_rights_expiration, 'null') as datetime) as lien_rights_expiration,
    cast(nullif(replace(replace(labor_cost, '$', ''), ',', ''), 'null') as float64) as labor_cost,
    class,
    invoice_number,
    cast(nullif(updated_date, 'null') as datetime) as updated_date,
    job_status,
    secondary_loss_type,
    cast(nullif(job_completion_percentage, 'null') as float64) as job_completion_percentage,
    cast(nullif(referral_fee_date_paid, 'null') as datetime) as referral_fee_date_paid
from {{ source("mail_exports", "fedex_kpi_revive") }}
where
    _partitiontime
    = (select max(_partitiontime) from {{ source("mail_exports", "fedex_kpi_revive") }})
