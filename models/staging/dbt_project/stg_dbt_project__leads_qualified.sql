with 

source as (

    select * from {{ source('dbt_project', 'leads_qualified') }}

),

renamed as (

    select
        _line,
        _fivetran_synced,
        mql_id,
        first_contact_date,
        landing_page_id,
        origin

    from source

)

select * from renamed
