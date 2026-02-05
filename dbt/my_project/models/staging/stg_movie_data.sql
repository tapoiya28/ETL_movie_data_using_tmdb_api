with source as (
    select * 
    from {{ source('raw_data', 'raw_movie_data') }}
), 
renamed as (
    select
        -- identifiers
        cast(id as integer) as movie_id,

        -- movie details
        cast(title as text) as title,
        cast(original_title as text) as original_title,
        cast(original_language as varchar) as original_language,
        COALESCE(NULLIF(release_date, '')::date, '1900-01-01'::date) as release_date,
        cast(status as text) as status,
        cast(runtime as integer) as runtime_minutes,

        -- categorization & flags
        cast(adult as boolean) as is_adult,
        cast(belongs_to_collection as boolean) as belongs_to_collection,

        -- quantitative metrics
        cast(popularity as numeric) as popularity,
        cast(budget as bigint) as budget,
        cast(revenue as numeric) as revenue,
        cast(vote_average as numeric) as average_rating,
        cast(vote_count as integer) as vote_count,
  
        -- metadata
        cast(extracted_at as timestamp) as extracted_at
    from source
)

select * from renamed
where movie_id is not null
