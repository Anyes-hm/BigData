with 

source as (

    select * from {{ source('dbt_project', 'customers') }}

),

renamed as (

    select
        _line,
        _fivetran_synced,
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state

    from source

)

select * from renamed
