with 

source as (

    select * from {{ source('dbt_project', 'order_payments') }}

),

renamed as (

    select
        _line,
        _fivetran_synced,
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value

    from source

)

select * from renamed
