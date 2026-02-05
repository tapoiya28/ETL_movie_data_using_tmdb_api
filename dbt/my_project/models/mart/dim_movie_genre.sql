with movie_genre as (
    select * 
    from {{ ref('stg_genre_data') }}
)

select * from movie_genre