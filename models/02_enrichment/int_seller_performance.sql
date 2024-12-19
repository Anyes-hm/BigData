with sellers as (

    select * from {{ ref('stg_dbt_project__sellers') }}
),

order_items as (

    select * from {{ ref('stg_dbt_project__order_items') }}
),

int_seller_performance as (
    select
        s.seller_id,
        count(distinct oi.order_id) as total_orders,
        round(sum(oi.price), 2) as total_revenue
    from sellers s
    left join order_items oi on s.seller_id = oi.seller_id
    group by s.seller_id
)

select * from int_seller_performance