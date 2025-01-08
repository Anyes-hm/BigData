WITH unique_customer_analysis AS (
    SELECT
        customer_unique_id,
        COUNT(customer_id) AS num_customer_ids -- customer_unique_id est attaché à combien de customer_id
    FROM {{ ref('stg_dbt_project__customers') }}
    GROUP BY customer_unique_id
)
SELECT
    COUNT(*) AS total_unique_customers, -- nombre total de customer_unique_id
    COUNT(CASE WHEN num_customer_ids = 1 THEN 1 END) AS unique_ids_with_1_record, -- Nombre de customer_unique_id avec 1 seul customer_id
    COUNT(CASE WHEN num_customer_ids > 1 THEN 1 END) AS unique_ids_with_multiple_records, -- Nombre de customer_unique_id avec plusieurs customer_id
    ROUND(AVG(num_customer_ids), 2) AS avg_customer_ids_per_unique_id -- Moyenne de customer_id par customer_unique_id
FROM unique_customer_analysis
