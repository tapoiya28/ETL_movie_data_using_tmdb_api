with country as (
    select * 
    from {{ ref('stg_country_data') }}
)

select * from country