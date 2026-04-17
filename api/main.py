import os
import logging
import difflib
from typing import Optional

import httpx
import psycopg2
import psycopg2.extras
import yt_dlp
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles

from neon_client import NeonClient

PG_DSN = os.environ.get("PG_DSN", "postgresql://postgres:postgres@postgres:5432/sampling_assiste")
NEON_DSN = os.environ.get("NEON_DSN", "")
DISCOGS_TOKEN = os.environ.get("DISCOGS_TOKEN", "")
DISCOGS_BASE_URL = "https://api.discogs.com"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="GemDigger")

INIT_LIKES_SQL = """
CREATE TABLE IF NOT EXISTS likes (
    discogs_id  INTEGER PRIMARY KEY,
    liked_at    TIMESTAMPTZ DEFAULT NOW()
);
"""


def get_neon() -> NeonClient:
    return NeonClient(NEON_DSN)


@app.on_event("startup")
def startup():
    """Crée la table likes sur Neon si elle n'existe pas."""
    try:
        neon = get_neon()
        neon.execute(INIT_LIKES_SQL)
        logger.info("Neon prêt.")
    except Exception as e:
        logger.error("Erreur Neon au startup : %s", e)


# ── Filters ─────────────────────────────────────────────────────────────────

@app.get("/api/filters")
def get_filters():
    neon = get_neon()
    genres = [r["genre"] for r in neon.execute("""
        SELECT DISTINCT unnest(genres) as genre
        FROM dim_releases
        WHERE genres IS NOT NULL
        ORDER BY genre
    """) if r["genre"] is not None]
    countries = [r["country"] for r in neon.execute("""
        SELECT DISTINCT country FROM dim_releases
        WHERE country IS NOT NULL AND country != '' ORDER BY country
    """)]
    rows = neon.execute("""
        SELECT MIN(year) as year_min, MAX(year) as year_max
        FROM dim_releases WHERE year IS NOT NULL
    """)
    year_min = rows[0]["year_min"] if rows else 1955
    year_max = rows[0]["year_max"] if rows else 1990

    return {"genres": genres, "countries": countries, "year_min": year_min, "year_max": year_max}


# ── Random release ───────────────────────────────────────────────────────────

@app.get("/api/random")
def get_random(
    popularity_min: int = Query(0, ge=0, le=100),
    popularity_max: int = Query(100, ge=0, le=100),
    year_min: Optional[int] = Query(None),
    year_max: Optional[int] = Query(None),
    genres: list[str] = Query(default=[]),
    countries: list[str] = Query(default=[]),
):
    conditions = [
        f"popularity_score >= {popularity_min}",
        f"popularity_score <= {popularity_max}",
    ]
    if year_min is not None:
        conditions.append(f"year >= {year_min}")
    if year_max is not None:
        conditions.append(f"year <= {year_max}")
    if genres:
        list_sql = ", ".join(f"'{g.replace(chr(39), chr(39)*2)}'" for g in genres)
        conditions.append(f"genres && ARRAY[{list_sql}]::text[]")
    if countries:
        list_sql = ", ".join(f"'{c.replace(chr(39), chr(39)*2)}'" for c in countries)
        conditions.append(f"country = ANY(ARRAY[{list_sql}])")

    where = " AND ".join(conditions)
    sql = f"""
        SELECT discogs_id, title, artist, year, country,
               genres, styles, label, popularity_score
        FROM dim_releases
        WHERE {where}
        ORDER BY RANDOM()
        LIMIT 1
    """
    neon = get_neon()
    rows = neon.execute(sql)
    if not rows:
        raise HTTPException(status_code=404, detail="Aucun album ne correspond aux critères.")
    return rows[0]


# ── Cover proxy ──────────────────────────────────────────────────────────────

@app.get("/api/cover/{discogs_id}")
async def get_cover(discogs_id: int):
    headers = {
        "Authorization": f"Discogs token={DISCOGS_TOKEN}",
        "User-Agent": "SamplingAssiste/0.1",
    }
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(f"{DISCOGS_BASE_URL}/releases/{discogs_id}", headers=headers)
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail="Discogs API error")
        data = resp.json()

    images = data.get("images", [])
    cover_url = images[0]["uri"] if images else None
    if not cover_url:
        raise HTTPException(status_code=404, detail="Pas de cover disponible")

    async with httpx.AsyncClient(timeout=15) as client:
        img_resp = await client.get(cover_url, headers=headers)
        content_type = img_resp.headers.get("content-type", "image/jpeg")
        return StreamingResponse(iter([img_resp.content]), media_type=content_type)


# ── Likes ────────────────────────────────────────────────────────────────────

@app.post("/api/like/{discogs_id}", status_code=201)
def add_like(discogs_id: int):
    neon = get_neon()
    neon.execute(f"INSERT INTO likes (discogs_id) VALUES ({discogs_id}) ON CONFLICT DO NOTHING")
    return {"liked": True}


@app.delete("/api/like/{discogs_id}")
def remove_like(discogs_id: int):
    neon = get_neon()
    neon.execute(f"DELETE FROM likes WHERE discogs_id = {discogs_id}")
    return {"liked": False}


@app.get("/api/likes")
def get_likes():
    neon = get_neon()
    return neon.execute("""
        SELECT r.discogs_id, r.title, r.artist, r.year, r.country,
               r.genres, r.styles, r.label, r.popularity_score,
               l.liked_at
        FROM likes l
        JOIN dim_releases r USING (discogs_id)
        ORDER BY l.liked_at DESC
    """)


# ── YouTube search ───────────────────────────────────────────────────────────

def _similarity(a: str, b: str) -> float:
    return difflib.SequenceMatcher(None, a.lower(), b.lower()).ratio()


@app.get("/api/youtube")
def search_youtube(artist: str, title: str, threshold: float = 0.45):
    query = f"{artist} {title}"
    ydl_opts = {"quiet": True, "no_warnings": True, "extract_flat": True}
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            result = ydl.extract_info(f"ytsearch3:{query}", download=False)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    entries = (result or {}).get("entries", [])
    if not entries:
        raise HTTPException(status_code=404, detail="Aucun résultat YouTube")

    for entry in entries:
        video_title = entry.get("title", "")
        score = _similarity(query, video_title)
        logger.info("YouTube candidat: %s (score=%.2f)", video_title, score)
        if score >= threshold:
            return {
                "video_id": entry["id"],
                "video_title": video_title,
                "score": round(score, 2),
                "view_count": entry.get("view_count"),
            }

    raise HTTPException(status_code=404, detail="Aucune vidéo suffisamment proche")


# ── Static frontend ───────────────────────────────────────────────────────────

app.mount("/", StaticFiles(directory="static", html=True), name="static")
