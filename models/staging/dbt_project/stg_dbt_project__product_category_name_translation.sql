with 

source as (

    select * from {{ source('dbt_project', 'product_category_name_translation') }}

),

renamed as (

    select
        _line,
        _fivetran_synced,
        product_category_name,
        product_category_name_english

    from source

)

select * from renamed
