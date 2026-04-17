with stg as (
    select * from {{ ref('stg_releases') }}
),

exploded as (
    -- Une ligne par genre pour calculer le max par genre
    select
        discogs_id,
        genre,
        (community_have + community_want) as community_sum
    from stg, unnest(genres) as genre
),

max_per_genre as (
    select genre, max(community_sum) as max_community_sum
    from exploded
    group by genre
),

best_max_per_release as (
    -- Pour chaque release, on prend le max_community_sum le plus élevé parmi ses genres
    -- → la release est comparée au genre où elle performe le mieux
    select e.discogs_id, max(m.max_community_sum) as best_max
    from exploded e
    join max_per_genre m on m.genre = e.genre
    group by e.discogs_id
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
        -- Popularité normalisée sur 100 : comparée au genre le plus favorable
        round(
            (s.community_have + s.community_want)::numeric
            / nullif(b.best_max, 0) * 100
        )                                                       as popularity_score,
        s.ingested_at
    from stg s
    left join best_max_per_release b on b.discogs_id = s.discogs_id
)

select * from enriched
