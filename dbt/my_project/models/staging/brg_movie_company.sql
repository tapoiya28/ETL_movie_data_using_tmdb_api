with source as (
    select *
    from {{ source('raw_data', 'raw_mov_comp_relation') }}
)

select * 
from source