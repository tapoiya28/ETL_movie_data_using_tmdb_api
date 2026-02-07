with source as (
    select *
    from {{ source('raw_data', 'raw_mov_gen_relation') }}
)

select * 
from source