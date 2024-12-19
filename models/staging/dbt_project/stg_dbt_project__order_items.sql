with 

source as (

    select * from {{ source('dbt_project', 'order_items') }}

),

renamed as (

    select
        _line,
        _fivetran_synced,
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date,
        price,
        freight_value

    from source

)

select * from renamed
