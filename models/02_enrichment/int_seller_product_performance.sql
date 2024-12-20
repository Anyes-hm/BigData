with int_seller_performance as (
    select * from {{ref('int_seller_performance')}}
),

oie as(
    select * from {{ref('int_order_items_enriched')}}
),

int_seller_product_performance as (
    select
        sp.seller_id,
        oie.product_id,
        count(distinct oie.order_id) as total_orders,
        count(oie.order_item_id) as total_items_sold,
        round(sum(oie.price), 2) as total_revenue,
        round(avg(oie.price), 2) as avg_item_price,
        sum(oie.freight_value) as total_freight_cost
    from int_seller_performance sp
    inner join oie 
        on sp.seller_id = oie.seller_id
    group by sp.seller_id, oie.product_id
)

select * from int_seller_product_performance