with movie as (
    select * from {{ ref('int_movie_data') }}
)

select * from movie