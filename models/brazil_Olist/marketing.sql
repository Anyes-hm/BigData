with marketing as(
    select
        lq.*,
        lc.*
        from {{ref('stg_dbt_project__leads_qualified')}} as lq
        JOIN {{ref('stg_dbt_project__leads_closed')}} as lc
        ON lq.mql_id=lc.mql_id
)

SELECT * from marketing