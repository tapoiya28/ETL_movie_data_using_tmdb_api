with source as (
    select * 
    from {{ source('raw_data', 'raw_movie_genre_data') }}
)

select 
    CAST(id AS INTEGER) as genre_id,
    COALESCE(CAST(name AS TEXT), 'None') as genre_name,
from source