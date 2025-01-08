SELECT
    s.seller_id,
    s.seller_city,
    s.seller_state,
    COUNT(cd.mql_id) AS total_deals_closed
FROM {{ ref('stg_dbt_project__leads_closed') }} AS cd
JOIN {{ ref('stg_dbt_project__sellers') }} AS s
    ON cd.seller_id = s.seller_id
GROUP BY s.seller_id, s.seller_city, s.seller_state
ORDER BY total_deals_closed DESC
