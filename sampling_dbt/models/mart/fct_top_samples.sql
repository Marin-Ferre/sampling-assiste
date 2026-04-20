with releases as (
    select * from {{ ref('dim_releases') }}
),

exploded as (
    select
        discogs_id,
        title,
        artist,
        year,
        decade,
        country,
        label,
        genre,
        styles,
        community_have,
        community_want,
        lowest_price,
        rarity_score,
        popularity_score,
        master_id
    from releases, unnest(genres) as genre
),

ranked as (
    select
        *,
        row_number() over (
            partition by genre, decade
            order by rarity_score desc, community_want desc
        ) as rank_in_genre_decade
    from exploded
    where community_want > 5  -- filtre le bruit (releases quasi-inconnues)
)

select * from ranked
