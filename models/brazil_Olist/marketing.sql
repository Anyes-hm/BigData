WITH marketing AS (
    SELECT
        lq.mql_id,
        lq.first_contact_date,
        lq.landing_page_id,
        lq.origin,
        lc.seller_id,
        lc.sdr_id,
        lc.won_date,
        lc.business_segment,
        lc.lead_type,
        lc.lead_behaviour_profile,
        lc.has_company,
        lc.has_gtin,
        lc.average_stock,
        lc.business_type,
        lc.declared_product_catalog_size,
        lc.declared_monthly_revenue
    FROM {{ ref('stg_dbt_project__leads_qualified') }} AS lq
    JOIN {{ ref('stg_dbt_project__leads_closed') }} AS lc
        ON lq.mql_id = lc.mql_id
)

SELECT * FROM marketing
