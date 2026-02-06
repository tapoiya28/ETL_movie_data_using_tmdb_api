with source as (
    select * 
    from {{ source('raw_data', 'raw_language_data') }}
)

select 
    CAST(iso_639_1 AS VARCHAR) as language_id,
    COALESCE(CAST(english_name AS TEXT), 'None') as en_language_name,
    COALESCE(CAST(name AS TEXT), 'None') as native_language_name
from source