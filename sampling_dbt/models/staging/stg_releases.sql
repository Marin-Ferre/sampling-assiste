with source as (
    select * from raw.releases
),

cleaned as (
    select
        discogs_id,
        nullif(trim(title), '')                         as title,
        nullif(trim(artist), '')                        as artist,
        case when year between 1900 and 2030 then year
             else null end                              as year,
        nullif(trim(country), '')                       as country,
        genres,
        styles,
        nullif(trim(label), '')                         as label,
        coalesce(community_have, 0)                     as community_have,
        coalesce(community_want, 0)                     as community_want,
        lowest_price,
        master_id,
        ingested_at,
        -- Décennie
        case when year between 1900 and 2030
             then (year / 10) * 10
             else null end                              as decade
    from source
    where title is not null
      and array_length(genres, 1) > 0
      and community_scraped_at is not null
)

select * from cleaned
