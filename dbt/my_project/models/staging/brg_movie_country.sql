with source as (
    select *
    from {{ source('raw_data', 'raw_mov_ctry_relation') }}
)

select * 
from source