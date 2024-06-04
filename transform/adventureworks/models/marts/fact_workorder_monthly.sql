select
    {{ dbt_utils.generate_surrogate_key(["dim_date.month_end_date", "product_key"]) }} as workorder_monthly_key,
    count(*) as workorder_count,
    dim_date.month_end_date as work_order_month_end_date,
    workorder.product_key
from {{ ref('fact_workorder') }} as workorder
inner join {{ ref('dim_date') }} as dim_date
    on workorder.startdate = dim_date.date_day
group by
    work_order_month_end_date,
    workorder.product_key
