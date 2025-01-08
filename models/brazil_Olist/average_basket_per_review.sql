WITH basket_totals AS (
    SELECT 
        oi.order_id,
        SUM(oi.price) AS basket_total  
    FROM {{ ref('stg_dbt_project__order_items') }} AS oi
    GROUP BY oi.order_id
),
basket_reviews AS (
    SELECT 
        bt.order_id,
        bt.basket_total,
        COUNT(orv.review_id) AS review_count  
    FROM basket_totals AS bt
    LEFT JOIN {{ ref('stg_dbt_project__order_reviews') }} AS orv
        ON bt.order_id = orv.order_id  
    GROUP BY bt.order_id, bt.basket_total
)
SELECT 
    SUM(basket_total) / SUM(review_count) AS average_basket_per_review  
FROM basket_reviews
