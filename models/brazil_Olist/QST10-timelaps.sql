WITH deal_dates AS (
    SELECT
        MIN(won_date) AS first_deal,
        MAX(won_date) AS last_deal
    FROM {{ref('stg_dbt_project__leads_closed')}}
)
SELECT
    first_deal,
    last_deal,
    DATE_DIFF(last_deal, first_deal, DAY) AS timelapse_days
FROM deal_dates
