"""
Ingestion incrémentale via l'API Discogs.

Récupère les releases récentes par genre (triées par date d'ajout desc)
et s'arrête dès qu'on atteint des releases déjà connues ou la limite de pages.

Usage : python -m ingestion.api_incremental
"""

import logging
import time
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ingestion.config import DISCOGS_TOKEN, DISCOGS_BASE_URL, PG_DSN, TARGET_GENRES

logger = logging.getLogger(__name__)

RATE_LIMIT_DELAY = 1.1   # 60 req/min max
MAX_PAGES_PER_GENRE = 10  # 1 000 releases max par genre par run
PAGE_SIZE = 100

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
    community_have = EXCLUDED.community_have,
    community_want = EXCLUDED.community_want,
    lowest_price   = EXCLUDED.lowest_price,
    ingested_at    = NOW();
"""

CHECKPOINT_SQL = """
INSERT INTO raw.api_checkpoints (run_at) VALUES (NOW())
ON CONFLICT DO NOTHING;
"""

CHECKPOINT_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS raw.api_checkpoints (
    run_at TIMESTAMPTZ PRIMARY KEY
);
"""


def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({
        "Authorization": f"Discogs token={DISCOGS_TOKEN}",
        "User-Agent": "SamplingAssiste/0.1",
    })
    return session


def get_last_run(conn) -> datetime | None:
    with conn.cursor() as cur:
        cur.execute(CHECKPOINT_TABLE_SQL)
        cur.execute("SELECT MAX(run_at) FROM raw.api_checkpoints")
        row = cur.fetchone()
    conn.commit()
    return row[0] if row and row[0] else None


def get_known_ids(conn) -> set[int]:
    """Retourne les discogs_id déjà en base pour détecter les doublons."""
    with conn.cursor() as cur:
        cur.execute("SELECT discogs_id FROM raw.releases")
        return {row[0] for row in cur.fetchall()}


def fetch_genre(session: requests.Session, genre: str, known_ids: set[int], conn) -> int:
    inserted = 0
    for page in range(1, MAX_PAGES_PER_GENRE + 1):
        resp = session.get(
            f"{DISCOGS_BASE_URL}/database/search",
            params={
                "genre": genre,
                "type": "release",
                "sort": "added",
                "sort_order": "desc",
                "per_page": PAGE_SIZE,
                "page": page,
            },
            timeout=30,
        )

        remaining = int(resp.headers.get("X-Discogs-Ratelimit-Remaining", 10))
        if remaining < 5:
            logger.warning("Rate limit proche, pause 60s")
            time.sleep(60)
        else:
            time.sleep(RATE_LIMIT_DELAY)

        resp.raise_for_status()
        results = resp.json().get("results", [])
        if not results:
            break

        batch = []
        all_known = True
        for hit in results:
            rid = hit.get("id")
            if not rid:
                continue
            if rid not in known_ids:
                all_known = False
                genres = hit.get("genre", [])
                styles = hit.get("style", [])
                year_str = hit.get("year")
                year = int(year_str) if year_str and year_str.isdigit() else None
                batch.append({
                    "discogs_id": rid,
                    "title": hit.get("title", ""),
                    "artist": hit.get("title", "").split(" - ")[0] if " - " in hit.get("title", "") else "Unknown",
                    "year": year,
                    "country": hit.get("country"),
                    "genres": genres,
                    "styles": styles,
                    "label": hit.get("label", [None])[0] if hit.get("label") else None,
                    "community_have": 0,
                    "community_want": 0,
                    "lowest_price": None,
                    "master_id": hit.get("master_id"),
                })
                known_ids.add(rid)

        if batch:
            psycopg2.extras.execute_batch(conn.cursor(), UPSERT_SQL, batch)
            conn.commit()
            inserted += len(batch)
            logger.info("Genre=%s page=%d — %d nouvelles releases", genre, page, len(batch))

        # Si toutes les releases de la page sont déjà connues, on arrête
        if all_known:
            logger.info("Genre=%s — releases déjà connues atteintes, arrêt page %d", genre, page)
            break

    return inserted


def run() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")

    conn = psycopg2.connect(PG_DSN)
    session = _make_session()

    logger.info("Chargement des IDs connus...")
    known_ids = get_known_ids(conn)
    logger.info("%d releases déjà en base", len(known_ids))

    total = 0
    for genre in TARGET_GENRES:
        logger.info("Ingestion API incrémentale — genre : %s", genre)
        total += fetch_genre(session, genre, known_ids, conn)

    with conn.cursor() as cur:
        cur.execute(CHECKPOINT_TABLE_SQL)
        cur.execute("INSERT INTO raw.api_checkpoints (run_at) VALUES (NOW())")
    conn.commit()
    conn.close()

    logger.info("Terminé — %d nouvelles releases ingérées", total)


if __name__ == "__main__":
    run()
