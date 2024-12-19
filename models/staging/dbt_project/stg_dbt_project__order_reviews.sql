with 

source as (

    select * from {{ source('dbt_project', 'order_reviews') }}

),

renamed as (

    select
        _line,
        _fivetran_synced,
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp

    from source

)

select * from renamed
