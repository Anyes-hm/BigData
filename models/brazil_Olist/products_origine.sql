WITH product_seller_data AS (
    SELECT 
        s.seller_id,
        s.seller_city,
        oi.price AS turnover_price,
        oi.order_item_id AS turnover_quantity  
    FROM {{ ref('stg_dbt_project__order_items') }} AS oi
    JOIN {{ ref('stg_dbt_project__sellers') }} AS s
        ON oi.seller_id = s.seller_id
),

aggregated_data AS (
    SELECT 
        seller_city,
        SUM(turnover_price) AS total_turnover_price,
        COUNT(turnover_quantity) AS total_turnover_quantity 
    FROM product_seller_data
    GROUP BY seller_city
)
SELECT 
    seller_city, 
    total_turnover_price, 
    total_turnover_quantity
FROM aggregated_data
