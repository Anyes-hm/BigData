with 

source as (

    select * from {{ source('dbt_project', 'orders') }}

),

renamed as (

    select
        _line,
        _fivetran_synced,
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date

    from source

)

select * from renamed
