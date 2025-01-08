WITH review_timelapses AS (
    SELECT 
        o.order_id,
        r.review_score,
        DATE_DIFF(o.order_delivered_customer_date, o.order_estimated_delivery_date, DAY) AS delivery_vs_estimate, 
        DATE_DIFF(o.order_delivered_customer_date, o.order_purchase_timestamp, DAY) AS purchase_to_delivery 
    FROM {{ ref('stg_dbt_project__orders') }} AS o
    JOIN {{ ref('stg_dbt_project__order_reviews') }} AS r
        ON o.order_id = r.order_id
    WHERE o.order_delivered_customer_date IS NOT NULL 
      AND o.order_estimated_delivery_date IS NOT NULL
      AND o.order_purchase_timestamp IS NOT NULL
)
SELECT 
    review_score,
    AVG(delivery_vs_estimate) AS avg_delivery_vs_estimate, 
    AVG(purchase_to_delivery) AS avg_purchase_to_delivery  
FROM review_timelapses
GROUP BY review_score
ORDER BY review_score
