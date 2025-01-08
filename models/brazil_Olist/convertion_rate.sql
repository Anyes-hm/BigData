WITH conversion_data AS (
    SELECT
        (SELECT COUNT(*) FROM {{ref('stg_dbt_project__leads_qualified')}}) AS qualified_leads,
        (SELECT COUNT(*) FROM {{ref('stg_dbt_project__leads_closed')}}) AS closed_deals
)
SELECT
    qualified_leads,
    closed_deals,
    (closed_deals * 100.0) / qualified_leads AS conversion_rate
FROM conversion_data