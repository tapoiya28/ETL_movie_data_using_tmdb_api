with language as (
    select * 
    from {{ ref('stg_language_data') }}
)

select * from language