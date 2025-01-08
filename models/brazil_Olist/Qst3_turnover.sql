WITH order_data AS (
    SELECT 
        oi.order_id,
        oi.product_id,
        oi.price AS item_price,
        o.customer_id
    FROM {{ ref('stg_dbt_project__order_items') }} AS oi
    JOIN {{ ref('stg_dbt_project__orders') }} AS o
        ON oi.order_id = o.order_id
),
customer_data AS (
    SELECT 
        c.customer_id,
        c.customer_city
    FROM {{ ref('stg_dbt_project__customers') }} AS c
),
product_data AS (
    SELECT 
        pc.product_id,
        pc.category_name_english
    FROM {{ ref('products_category') }} AS pc
),
joined_data AS (
    SELECT 
        cd.customer_city,
        pd.category_name_english,
        od.item_price,
        od.order_id,
        cd.customer_id
    FROM order_data AS od
    JOIN customer_data AS cd
        ON od.customer_id = cd.customer_id
    JOIN product_data AS pd
        ON od.product_id = pd.product_id
)
SELECT 
    customer_city, 
    category_name_english, 
    SUM(item_price) AS total_turnover, 
    COUNT(DISTINCT order_id) AS total_orders, 
    COUNT(DISTINCT customer_id) AS total_customers
FROM joined_data
GROUP BY customer_city, category_name_english
