WITH products_category AS (
    SELECT
        p.product_id,
        t.product_category_name_english AS category_name_english
    FROM {{ ref('stg_dbt_project__products') }} p
    LEFT JOIN {{ ref('stg_dbt_project__product_category_name_translation') }} t
        ON p.product_category_name = t.product_category_name
)
SELECT *
FROM products_category
