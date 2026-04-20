"""
Enrichissement community.have / community.want via l'API Discogs.

Les dumps XML ne contiennent pas ces données — on les récupère
release par release via /releases/{id} (60 req/min authentifié).

Checkpoint : colonne community_scraped_at sur raw.releases.
Reprise automatique sur les releases non encore enrichies.

Usage : python -m ingestion.community_enricher [--batch 50000]
"""

import argparse
import json
import logging
import time
from pathlib import Path

import psycopg2
import psycopg2.extras
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ingestion.config import DISCOGS_TOKEN, DISCOGS_BASE_URL, PG_DSN

logger = logging.getLogger(__name__)

BATCH_SIZE = 200  # releases chargées en mémoire à la fois

# Fichier de stats partagé avec le monitor
STATS_FILE = Path(__file__).parent.parent / "data" / "enricher_stats.json"


def _write_stats(stats: dict) -> None:
    STATS_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATS_FILE.write_text(json.dumps(stats))

# Critères de priorité — enrichies en premier
PRIORITY_GENRES = ["Funk / Soul", "Jazz", "Reggae", "Latin", "Blues"]
PRIORITY_STYLES = ["Afrobeat", "Soul", "Funk", "Jazz-Funk", "Roots Reggae", "Dub"]
PRIORITY_YEAR_MIN = 1960
PRIORITY_YEAR_MAX = 1990
PRIORITY_COUNTRIES = ["US", "Jamaica", "Nigeria", "France", "UK", "Brazil", "Cuba"]

UPDATE_SQL = """
UPDATE raw.releases SET
    community_have       = %(have)s,
    community_want       = %(want)s,
    lowest_price         = %(lowest_price)s,
    community_scraped_at = NOW()
WHERE discogs_id = %(discogs_id)s;
"""

ENSURE_COLUMN_SQL = """
ALTER TABLE raw.releases
    ADD COLUMN IF NOT EXISTS community_scraped_at TIMESTAMPTZ;
"""


def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers.update({
        "Authorization": f"Discogs token={DISCOGS_TOKEN}",
        "User-Agent": "SamplingAssiste/0.1",
    })
    return session


def fetch_community(
    session: requests.Session,
    discogs_id: int,
    stats: dict,
) -> dict | None:
    """Appelle /releases/{id} et retourne have, want, lowest_price."""
    try:
        t0 = time.time()
        resp = session.get(
            f"{DISCOGS_BASE_URL}/releases/{discogs_id}",
            timeout=30,
        )
        elapsed = time.time() - t0

        stats["total_requests"] += 1
        stats["last_request_at"] = time.time()

        # Respect du rate limit via header Discogs
        limit = int(resp.headers.get("X-Discogs-Ratelimit-Limit", 60))
        remaining = int(resp.headers.get("X-Discogs-Ratelimit-Remaining", 10))
        stats["rate_limit_total"] = limit
        stats["rate_limit_remaining"] = remaining
        stats["rate_limit_used"] = limit - remaining

        if remaining < 3:
            logger.warning("Rate limit proche (%d), pause 60s", remaining)
            stats["pauses"] = stats.get("pauses", 0) + 1
            _write_stats(stats)
            time.sleep(60)
        elif remaining < 7:
            time.sleep(max(0, 2.0 - elapsed))
        else:
            # On soustrait le temps de la requête pour tenir ~60 req/min
            time.sleep(max(0, 1.0 - elapsed))

        _write_stats(stats)

        if resp.status_code == 404:
            stats["errors_404"] = stats.get("errors_404", 0) + 1
            return {"have": 0, "want": 0, "lowest_price": None}

        resp.raise_for_status()
        data = resp.json()
        community = data.get("community", {})
        return {
            "have":        community.get("have", 0),
            "want":        community.get("want", 0),
            "lowest_price": data.get("lowest_price"),
        }

    except Exception as exc:
        logger.warning("Erreur release %d : %s", discogs_id, exc)
        stats["errors_other"] = stats.get("errors_other", 0) + 1
        _write_stats(stats)
        return None


def get_pending_ids(conn, limit: int) -> list[int]:
    """Retourne les discogs_id non encore enrichis, releases prioritaires en premier."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT discogs_id FROM raw.releases
            WHERE community_scraped_at IS NULL
            ORDER BY
                CASE WHEN
                    genres && %s
                    AND year BETWEEN %s AND %s
                    AND country = ANY(%s)
                THEN 0 ELSE 1 END,
                discogs_id
            LIMIT %s
        """, (
            PRIORITY_GENRES,
            PRIORITY_YEAR_MIN,
            PRIORITY_YEAR_MAX,
            PRIORITY_COUNTRIES,
            limit,
        ))
        return [row[0] for row in cur.fetchall()]


def get_progress(conn) -> tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE community_scraped_at IS NOT NULL) as done,
                COUNT(*) as total
            FROM raw.releases
        """)
        return cur.fetchone()


def run(batch_limit: int | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")

    conn = psycopg2.connect(PG_DSN)
    session = _make_session()

    # S'assure que la colonne existe
    with conn.cursor() as cur:
        cur.execute(ENSURE_COLUMN_SQL)
    conn.commit()

    done, total = get_progress(conn)
    logger.info("Progression : %d / %d enrichies (%.1f%%)", done, total, done / total * 100 if total else 0)

    enriched = 0
    limit = batch_limit or total

    stats = {
        "started_at": time.time(),
        "total_requests": 0,
        "enriched_session": 0,
        "enriched_total": done,
        "releases_total": total,
        "rate_limit_total": 60,
        "rate_limit_remaining": 60,
        "rate_limit_used": 0,
        "pauses": 0,
        "errors_404": 0,
        "errors_other": 0,
        "last_request_at": None,
        "last_title": None,
        "last_artist": None,
    }
    _write_stats(stats)

    while enriched < limit:
        ids = get_pending_ids(conn, min(BATCH_SIZE, limit - enriched))
        if not ids:
            logger.info("Toutes les releases sont enrichies.")
            break

        updates = []
        for discogs_id in ids:
            community = fetch_community(session, discogs_id, stats)
            if community is None:
                continue
            updates.append({"discogs_id": discogs_id, **community})

        if updates:
            with conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, UPDATE_SQL, updates)
            conn.commit()
            enriched += len(updates)
            done_now, _ = get_progress(conn)

            with conn.cursor() as cur:
                cur.execute("""
                    SELECT title, artist FROM raw.releases
                    WHERE community_scraped_at IS NOT NULL
                    ORDER BY community_scraped_at DESC LIMIT 1
                """)
                row = cur.fetchone()
                if row:
                    stats["last_title"] = row[0]
                    stats["last_artist"] = row[1]

            stats["enriched_session"] = enriched
            stats["enriched_total"] = done_now
            _write_stats(stats)

            logger.info(
                "Enrichies : %d | Session : %d | Total : %d / %d (%.1f%%)",
                len(updates), enriched, done_now, total,
                done_now / total * 100 if total else 0,
            )

    stats["finished_at"] = time.time()
    _write_stats(stats)
    conn.close()
    logger.info("Termine — %d releases enrichies cette session", enriched)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", type=int, default=None,
                        help="Nombre max de releases a enrichir (defaut : toutes)")
    args = parser.parse_args()
    run(batch_limit=args.batch)
