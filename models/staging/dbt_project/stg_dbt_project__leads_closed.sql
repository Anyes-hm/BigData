with 

source as (

    select * from {{ source('dbt_project', 'leads_closed') }}

),

renamed as (

    select
        _line,
        _fivetran_synced,
        mql_id,
        seller_id,
        sdr_id,
        sr_id,
        won_date,
        business_segment,
        lead_type,
        lead_behaviour_profile,
        has_company,
        has_gtin,
        average_stock,
        business_type,
        declared_product_catalog_size,
        declared_monthly_revenue

    from source

)

select * from renamed
