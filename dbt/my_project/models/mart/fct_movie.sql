with movie as (
    select * from {{ ref('int_movie_data') }}
)

select 
    movie_key,
        
    -- identifiers
    movie_id,
    original_language,

    -- movie details
    title,
    original_title,
    release_date,
    release_month,
    release_year,
    status,
    runtime_minutes,

    -- categorization & flags
    is_adult,
    belongs_to_collection,

    -- quantitative metrics
    popularity,
    average_rating,
    vote_count,

    budget,
    revenue,
    profit,
    roi_percentage
from movie m