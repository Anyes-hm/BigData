WITH basket_totals AS (
    SELECT 
        oi.order_id,  
        SUM(oi.price) AS basket_total  
    FROM {{ ref('stg_dbt_project__order_items') }} AS oi
    GROUP BY oi.order_id  
)
SELECT 
    AVG(basket_total) AS average_basket  
FROM basket_totals
