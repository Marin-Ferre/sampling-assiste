"""
Script principal d'ingestion Discogs → PostgreSQL.

Stratégie :
  - Parcourt les releases par genre via /database/search
  - Enrichit chaque release via /releases/{id} pour récupérer
    community.have/want, lowest_price, master_id
  - Insère en upsert dans la table raw.releases
  - Checkpoint par (genre, page) pour reprise sur erreur
"""

import logging
import sys
from dataclasses import dataclass, field

import psycopg2
import psycopg2.extras

from ingestion import discogs_client as dc
from ingestion.config import PG_DSN, TARGET_GENRES, INGESTION_PAGE_SIZE

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Modèle de données
# ---------------------------------------------------------------------------

@dataclass
class Release:
    discogs_id: int
    title: str
    artist: str
    year: int | None
    country: str | None
    genres: list[str] = field(default_factory=list)
    styles: list[str] = field(default_factory=list)
    label: str | None = None
    community_have: int = 0
    community_want: int = 0
    lowest_price: float | None = None
    master_id: int | None = None


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------

def _extract_artist(data: dict) -> str:
    """Concatène les noms d'artistes (join sur ' / ')."""
    artists = data.get("artists", [])
    if artists:
        return " / ".join(a.get("name", "").strip() for a in artists)
    return data.get("artist", "") or "Unknown"


def _extract_label(data: dict) -> str | None:
    labels = data.get("labels", [])
    return labels[0].get("name") if labels else None


def fetch_release_detail(release_id: int) -> dict:
    """Appel /releases/{id} pour les champs community et lowest_price."""
    return dc.get(f"/releases/{release_id}")


def parse_release(search_hit: dict, detail: dict) -> Release:
    community = detail.get("community", {})
    return Release(
        discogs_id=detail["id"],
        title=detail.get("title", search_hit.get("title", "")),
        artist=_extract_artist(detail),
        year=detail.get("year"),
        country=detail.get("country"),
        genres=detail.get("genres", []),
        styles=detail.get("styles", []),
        label=_extract_label(detail),
        community_have=community.get("have", 0),
        community_want=community.get("want", 0),
        lowest_price=detail.get("lowest_price"),
        master_id=detail.get("master_id"),
    )


# ---------------------------------------------------------------------------
# Base de données
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.releases (
    discogs_id      INTEGER PRIMARY KEY,
    title           TEXT,
    artist          TEXT,
    year            SMALLINT,
    country         TEXT,
    genres          TEXT[],
    styles          TEXT[],
    label           TEXT,
    community_have  INTEGER,
    community_want  INTEGER,
    lowest_price    NUMERIC(10, 2),
    master_id       INTEGER,
    ingested_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.ingestion_checkpoints (
    genre   TEXT,
    page    INTEGER,
    PRIMARY KEY (genre, page)
);
"""

UPSERT_SQL = """
INSERT INTO raw.releases (
    discogs_id, title, artist, year, country,
    genres, styles, label,
    community_have, community_want, lowest_price, master_id
) VALUES (
    %(discogs_id)s, %(title)s, %(artist)s, %(year)s, %(country)s,
    %(genres)s, %(styles)s, %(label)s,
    %(community_have)s, %(community_want)s, %(lowest_price)s, %(master_id)s
)
ON CONFLICT (discogs_id) DO UPDATE SET
    community_have  = EXCLUDED.community_have,
    community_want  = EXCLUDED.community_want,
    lowest_price    = EXCLUDED.lowest_price,
    ingested_at     = NOW();
"""


def init_db(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()
    logger.info("Schéma initialisé")


def is_page_done(conn, genre: str, page: int) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM raw.ingestion_checkpoints WHERE genre=%s AND page=%s",
            (genre, page),
        )
        return cur.fetchone() is not None


def mark_page_done(conn, genre: str, page: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO raw.ingestion_checkpoints (genre, page) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (genre, page),
        )
    conn.commit()


def upsert_releases(conn, releases: list[Release]) -> None:
    rows = [
        {
            "discogs_id": r.discogs_id,
            "title": r.title,
            "artist": r.artist,
            "year": r.year,
            "country": r.country,
            "genres": r.genres,
            "styles": r.styles,
            "label": r.label,
            "community_have": r.community_have,
            "community_want": r.community_want,
            "lowest_price": r.lowest_price,
            "master_id": r.master_id,
        }
        for r in releases
    ]
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, UPSERT_SQL, rows)
    conn.commit()


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

def ingest_genre(conn, genre: str) -> None:
    logger.info("Début ingestion genre : %s", genre)
    page = 1

    while True:
        if is_page_done(conn, genre, page):
            logger.debug("Page %d/%s déjà traitée, skip", page, genre)
            page += 1
            continue

        logger.info("Genre=%s page=%d", genre, page)
        data = dc.get(
            "/database/search",
            params={
                "genre": genre,
                "type": "release",
                "per_page": INGESTION_PAGE_SIZE,
                "page": page,
            },
        )

        results = data.get("results", [])
        if not results:
            logger.info("Fin des résultats pour %s à la page %d", genre, page)
            break

        releases: list[Release] = []
        for hit in results:
            release_id = hit.get("id")
            if not release_id:
                continue
            try:
                detail = fetch_release_detail(release_id)
                releases.append(parse_release(hit, detail))
            except Exception as exc:
                logger.warning("Erreur release %s : %s", release_id, exc)

        upsert_releases(conn, releases)
        mark_page_done(conn, genre, page)
        logger.info("Page %d insérée (%d releases)", page, len(releases))

        pagination = data.get("pagination", {})
        if page >= pagination.get("pages", 1):
            break
        page += 1


def run(genres: list[str] | None = None) -> None:
    targets = genres or TARGET_GENRES
    conn = psycopg2.connect(PG_DSN)
    try:
        init_db(conn)
        for genre in targets:
            ingest_genre(conn, genre)
    finally:
        conn.close()
    logger.info("Ingestion terminée")


if __name__ == "__main__":
    genres_arg = sys.argv[1:] or None
    run(genres_arg)
