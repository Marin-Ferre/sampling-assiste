"""
Parse le dump XML Discogs releases et insère en bulk dans PostgreSQL.

- Streaming XML (lxml iterparse) — mémoire constante quelle que soit la taille
- Filtre sur TARGET_GENRES
- Bulk insert par batch de 500 via COPY
- Checkpoint par offset pour reprise sur erreur
- lowest_price absent des dumps → NULL (enrichissement API optionnel)

Usage : python -m ingestion.dump_parser [chemin_dump.xml.gz]
"""

import gzip
import io
import logging
import os
import sys
from pathlib import Path

import psycopg2
import psycopg2.extras
from lxml import etree
from tqdm import tqdm

from ingestion.config import PG_DSN, TARGET_GENRES, YEAR_MIN, YEAR_MAX

logger = logging.getLogger(__name__)

BATCH_SIZE = 500
TARGET_GENRES_SET = {g.lower() for g in TARGET_GENRES}

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
    ingested_at = NOW();
"""

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

CREATE TABLE IF NOT EXISTS raw.dump_checkpoints (
    dump_file   TEXT PRIMARY KEY,
    offset_done BIGINT DEFAULT 0,
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);
"""


def init_db(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()
    logging.getLogger(__name__).info("Schéma initialisé")


# ---------------------------------------------------------------------------
# Helpers XML
# ---------------------------------------------------------------------------

def _text(el, tag: str) -> str | None:
    child = el.find(tag)
    return child.text.strip() if child is not None and child.text else None


def _int(el, tag: str) -> int | None:
    val = _text(el, tag)
    try:
        return int(val) if val else None
    except ValueError:
        return None


def parse_release(el) -> dict | None:
    """Extrait les champs d'un élément <release>. Retourne None si hors critères."""
    # Filtre genre
    genres = [g.text.strip() for g in el.findall("genres/genre") if g.text]
    if not any(g.lower() in TARGET_GENRES_SET for g in genres):
        return None

    # Filtre année — on parse l'année tôt pour éviter le reste du travail
    year_text = _text(el, "released")
    year = None
    if year_text:
        try:
            parsed = int(year_text[:4])
            if parsed > 0:
                year = parsed
        except ValueError:
            pass
    # Si l'année est connue et hors plage, on exclut.
    # Si l'année est inconnue (None), on garde — mieux vaut inclure que rater des classiques.
    if year is not None and not (YEAR_MIN <= year <= YEAR_MAX):
        return None

    styles = [s.text.strip() for s in el.findall("styles/style") if s.text]

    artists = el.findall("artists/artist/name")
    artist = " / ".join(a.text.strip() for a in artists if a.text) or "Unknown"

    labels = el.findall("labels/label")
    label = labels[0].get("name") if labels else None

    community = el.find("community")
    have = int(community.findtext("have") or 0) if community is not None else 0
    want = int(community.findtext("want") or 0) if community is not None else 0

    master_el = el.find("master_id")
    master_id = int(master_el.text) if master_el is not None and master_el.text else None

    return {
        "discogs_id": int(el.get("id")),
        "title": _text(el, "title"),
        "artist": artist,
        "year": year,
        "country": _text(el, "country"),
        "genres": genres,
        "styles": styles,
        "label": label,
        "community_have": have,
        "community_want": want,
        "lowest_price": None,  # absent des dumps
        "master_id": master_id,
    }


# ---------------------------------------------------------------------------
# Checkpoint
# ---------------------------------------------------------------------------

def get_checkpoint(conn, dump_file: str) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT offset_done FROM raw.dump_checkpoints WHERE dump_file=%s", (dump_file,))
        row = cur.fetchone()
    conn.commit()
    return row[0] if row else 0


def save_checkpoint(conn, dump_file: str, offset: int) -> None:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO raw.dump_checkpoints (dump_file, offset_done)
            VALUES (%s, %s)
            ON CONFLICT (dump_file) DO UPDATE SET
                offset_done = EXCLUDED.offset_done,
                updated_at  = NOW()
        """, (dump_file, offset))
    conn.commit()


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

def parse_and_insert(dump_path: Path, conn) -> None:
    dump_file = dump_path.name
    checkpoint = get_checkpoint(conn, dump_file)

    if checkpoint:
        logger.info("Reprise depuis l'offset %d", checkpoint)

    total_size = dump_path.stat().st_size
    batch: list[dict] = []
    processed = 0
    inserted = 0

    with (
        gzip.open(dump_path, "rb") as gz,
        tqdm(total=total_size, unit="B", unit_scale=True, desc="Parsing dump") as bar,
    ):
        # Avance jusqu'au checkpoint si reprise
        if checkpoint:
            gz.seek(checkpoint)
            bar.update(checkpoint)

        context = etree.iterparse(gz, events=("end",), tag="release", recover=True)

        for _, el in context:
            processed += 1

            record = parse_release(el)
            if record:
                batch.append(record)

            # Libère la mémoire — critique pour les gros fichiers
            el.clear()
            while el.getprevious() is not None:
                del el.getparent()[0]

            if len(batch) >= BATCH_SIZE:
                psycopg2.extras.execute_batch(conn.cursor(), UPSERT_SQL, batch)
                conn.commit()
                inserted += len(batch)

                offset = gz.tell()
                save_checkpoint(conn, dump_file, offset)
                bar.update(offset - bar.n)

                logger.info(
                    "Releases traitées : %d | Insérées (dans genre) : %d | Offset : %d MB",
                    processed, inserted, offset // 1_000_000,
                )
                batch.clear()

        # Dernier batch
        if batch:
            psycopg2.extras.execute_batch(conn.cursor(), UPSERT_SQL, batch)
            conn.commit()
            inserted += len(batch)

    logger.info("Terminé — %d releases traitées, %d insérées", processed, inserted)


def run(dump_path: Path | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")

    if dump_path is None:
        DUMPS_DIR = Path(__file__).parent.parent / "data" / "dumps"
        dumps = sorted(DUMPS_DIR.glob("discogs_*_releases.xml.gz"))
        if not dumps:
            raise FileNotFoundError(f"Aucun dump trouvé dans {DUMPS_DIR}. Lance d'abord dump_downloader.py")
        dump_path = dumps[-1]

    logger.info("Dump : %s (%.1f GB)", dump_path.name, dump_path.stat().st_size / 1e9)
    logger.info("Genres cibles : %s", TARGET_GENRES)

    conn = psycopg2.connect(PG_DSN)
    try:
        init_db(conn)
        parse_and_insert(dump_path, conn)
    finally:
        conn.close()


if __name__ == "__main__":
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else None
    run(path)
