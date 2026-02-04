with source as (
    select * 
    from {{ source('raw_data', 'raw_country_data') }}
)

select 
    CAST(iso_3166_1 AS varchar) as id,
    COALESCE(CAST(english_name AS TEXT), 'None') as en_country_name,
    COALESCE(CAST(native_name AS TEXT), 'None') as native_country_name
from source