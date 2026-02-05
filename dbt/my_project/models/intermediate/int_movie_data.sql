with movie_data as (
    select * 
    from {{ ref('stg_movie_data') }}
), 
cleaned as (
    select
        -- surrogate key
        {{ 
            dbt_utils.generate_surrogate_key(
                [ 'movie_id', 'title', 'release_date']
            ) 
        }} as movie_key,
        
        -- identifiers
        movie_id,

        -- movie details
        trim(lower(title)) as title,
        trim(lower(original_title)) as original_title,
        trim(lower(original_language)) as original_language,
        release_date,
        CAST(extract(month from release_date) AS INTEGER) as release_month,
        CAST(extract(year from release_date) AS INTEGER) as release_year,
        lower(trim(status)) as status,
        runtime_minutes,

        -- categorization & flags
        is_adult,
        belongs_to_collection,

        -- quantitative metrics
        COALESCE(popularity, 0) as popularity,
        COALESCE(average_rating, 0) as average_rating,
        COALESCE(vote_count, 0) as vote_count,

        COALESCE(budget, 0) as budget,
        COALESCE(revenue, 0) as revenue,
        (revenue - budget) as profit,

        case 
            when budget > 0 then round((revenue/budget) * 100, 2)
            else 0
        end as roi_percentage,
        
        -- metadata
        extracted_at,
        row_number() over (
            partition by movie_id
            order by extracted_at desc
        ) as rn

    from movie_data
)

select * 
from cleaned
where rn = 1 
    and release_year != 1900