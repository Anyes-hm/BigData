SELECT COUNT(*) AS deals_closed_on_saturday
FROM {{ ref('stg_dbt_project__leads_closed') }}
WHERE EXTRACT(DAYOFWEEK FROM won_date) = 7
