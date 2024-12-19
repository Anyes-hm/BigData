with order_items as (

    select * from {{ ref('stg_dbt_project__order_items') }}
),

products as (
    
    select * from {{ ref('stg_dbt_project__products') }}
),

int_order_items_enriched as (
    select
        oi.order_id,
        oi.order_item_id,
        oi.product_id,
        oi.seller_id,
        oi.shipping_limit_date,
        oi.price,
        oi.freight_value,
        p.product_category_name,
        p.product_name_lenght,
        p.product_description_lenght,
        p.product_photos_qty,
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm
    from order_items oi
    left join products p
    on oi.product_id = p.product_id
)

select * from int_order_items_enriched