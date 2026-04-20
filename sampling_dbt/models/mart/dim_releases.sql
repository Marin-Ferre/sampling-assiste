with stg as (
    select * from {{ ref('stg_releases') }}
),

exploded as (
    -- Une ligne par genre pour calculer le percentile rank par genre
    select
        discogs_id,
        genre,
        (community_have + community_want) as community_sum
    from stg, unnest(genres) as genre
),

percentile_per_genre as (
    -- Percentile rank de chaque release dans chaque genre
    select
        discogs_id,
        genre,
        round(
            percent_rank() over (
                partition by genre
                order by community_sum
            ) * 100
        ) as pct_rank
    from exploded
),

best_pct_per_release as (
    -- Pour chaque release, on garde le meilleur percentile parmi ses genres
    select discogs_id, max(pct_rank) as popularity_score
    from percentile_per_genre
    group by discogs_id
),

enriched as (
    select
        s.discogs_id,
        s.title,
        s.artist,
        s.year,
        s.decade,
        s.country,
        s.genres,
        s.styles,
        s.label,
        s.community_have,
        s.community_want,
        s.lowest_price,
        s.master_id,
        -- Somme brute have + want
        (s.community_have + s.community_want)                   as community_sum,
        -- Score de rareté : fort want, faible have = morceau rare et recherché
        case
            when s.community_have > 0
            then round(s.community_want::numeric / s.community_have, 4)
            else s.community_want::numeric
        end                                                     as rarity_score,
        -- Popularité : percentile rank → distribution uniforme par genre
        p.popularity_score,
        s.ingested_at
    from stg s
    left join best_pct_per_release p on p.discogs_id = s.discogs_id
)

select * from enriched
