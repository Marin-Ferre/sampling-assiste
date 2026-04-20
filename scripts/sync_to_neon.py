"""
Synchronise dim_releases depuis PostgreSQL local vers Neon (via HTTP).

Usage : python scripts/sync_to_neon.py
Lancer après chaque dbt run pour mettre à jour l'app.

Schema allégé pour scalabilité : ~150 bytes/release → ~150MB pour 1M releases.
"""

import httpx
import logging
import psycopg2
import psycopg2.extras

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")
logger = logging.getLogger(__name__)

import os
LOCAL_DSN = os.environ.get("PG_DSN", "postgresql://postgres:postgres@postgres:5432/sampling_assiste")
NEON_HOST = os.environ.get("NEON_HOST", "REDACTED_NEON_HOST")
NEON_USER = os.environ.get("NEON_USER", "REDACTED_NEON_USER")
NEON_PASSWORD = os.environ.get("NEON_PASSWORD", "")

NEON_HTTP_URL = f"https://{NEON_HOST}/sql"
HEADERS = {
    "Content-Type": "application/json",
    "Neon-Connection-String": f"postgresql://{NEON_USER}:{NEON_PASSWORD}@{NEON_HOST}/neondb",
}

BATCH_SIZE = 500


def neon_exec(client: httpx.Client, query: str):
    resp = client.post(NEON_HTTP_URL, json={"query": query}, headers=HEADERS, timeout=60)
    if not resp.is_success:
        logger.error("Neon error %d: %s", resp.status_code, resp.text[:500])
    resp.raise_for_status()
    return resp.json()


def sync():
    logger.info("Connexion à PostgreSQL local...")
    conn = psycopg2.connect(LOCAL_DSN)

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT COUNT(*) as n FROM mart.dim_releases")
        total = cur.fetchone()["n"]
        logger.info("%d releases à synchroniser vers Neon", total)

        cur.execute("""
            SELECT discogs_id, title, artist, year, country,
                   genres, styles, label, popularity_score
            FROM mart.dim_releases
        """)
        rows = cur.fetchall()
    conn.close()

    with httpx.Client(timeout=60) as client:
        logger.info("Création du schéma sur Neon...")
        neon_exec(client, "DROP TABLE IF EXISTS likes")
        neon_exec(client, "DROP TABLE IF EXISTS dim_releases")
        neon_exec(client, """
            CREATE TABLE dim_releases (
                discogs_id       INTEGER PRIMARY KEY,
                title            TEXT,
                artist           TEXT,
                year             SMALLINT,
                country          TEXT,
                genres           TEXT[],
                styles           TEXT[],
                label            TEXT,
                popularity_score NUMERIC
            );
        """)
        neon_exec(client, """
            CREATE TABLE likes (
                discogs_id  INTEGER PRIMARY KEY REFERENCES dim_releases(discogs_id) ON DELETE CASCADE,
                liked_at    TIMESTAMPTZ DEFAULT NOW()
            );
        """)

        inserted = 0
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i:i + BATCH_SIZE]
            values = []
            for r in batch:
                values.append(
                    f"({r['discogs_id']}, "
                    f"{_sql_str(r['title'])}, "
                    f"{_sql_str(r['artist'])}, "
                    f"{r['year'] or 'NULL'}, "
                    f"{_sql_str(r['country'])}, "
                    f"{_sql_array(r['genres'])}, "
                    f"{_sql_array(r['styles'])}, "
                    f"{_sql_str(r['label'])}, "
                    f"{r['popularity_score'] or 'NULL'})"
                )
            sql = "INSERT INTO dim_releases VALUES " + ", ".join(values) + " ON CONFLICT DO NOTHING"
            neon_exec(client, sql)
            inserted += len(batch)
            logger.info("Insérées : %d / %d", inserted, total)

    logger.info("Sync terminé — %d releases sur Neon", inserted)


def _sql_str(val) -> str:
    if val is None:
        return "NULL"
    escaped = str(val).replace("'", "''")
    return f"'{escaped}'"


def _sql_array(items) -> str:
    if not items:
        return "ARRAY[]::text[]"
    elems = ", ".join(f"'{str(i).replace(chr(39), chr(39)*2)}'" for i in items)
    return f"ARRAY[{elems}]::text[]"


if __name__ == "__main__":
    sync()
