with 

source as (

    select * from {{ source('dbt_project', 'geolocation') }}

),

renamed as (

    select
        _line,
        _fivetran_synced,
        geolocation_zip_code_prefix,
        geolocation_lat,
        geolocation_lng,
        geolocation_city,
        geolocation_state

    from source

)

select * from renamed
