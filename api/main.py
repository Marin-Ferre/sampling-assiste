import os
import logging
import difflib
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
import psycopg2
import psycopg2.extras
import yt_dlp
from fastapi import FastAPI, Query, HTTPException, Response, Cookie, Body
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel

from neon_client import NeonClient

PG_DSN = os.environ.get("PG_DSN", "postgresql://postgres:postgres@postgres:5432/sampling_assiste")
NEON_DSN = os.environ.get("NEON_DSN", "")
DISCOGS_TOKEN = os.environ.get("DISCOGS_TOKEN", "")
DISCOGS_BASE_URL = "https://api.discogs.com"
JWT_SECRET = os.environ.get("JWT_SECRET", "gemdigger-secret-change-in-prod")
JWT_ALGORITHM = "HS256"
JWT_EXPIRE_DAYS = 30

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="GemDigger")
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


# ── DB init ───────────────────────────────────────────────────────────────────

INIT_SQL = """
CREATE TABLE IF NOT EXISTS likes (
    discogs_id  INTEGER PRIMARY KEY,
    liked_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS users (
    id            SERIAL PRIMARY KEY,
    username      TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    created_at    TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS user_likes (
    user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    discogs_id  INTEGER NOT NULL,
    liked_at    TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, discogs_id)
);
"""


def get_neon() -> NeonClient:
    return NeonClient(NEON_DSN)


@app.on_event("startup")
def startup():
    try:
        neon = get_neon()
        for stmt in INIT_SQL.strip().split(";"):
            stmt = stmt.strip()
            if stmt:
                neon.execute(stmt)
        logger.info("Neon pret.")
    except Exception as e:
        logger.error("Erreur Neon au startup : %s", e)


# ── Auth helpers ──────────────────────────────────────────────────────────────

class RegisterBody(BaseModel):
    username: str
    password: str

class LoginBody(BaseModel):
    username: str
    password: str

def _hash_password(pw: str) -> str:
    return pwd_context.hash(pw)

def _verify_password(pw: str, hashed: str) -> bool:
    return pwd_context.verify(pw, hashed)

def _create_token(user_id: int, username: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(days=JWT_EXPIRE_DAYS)
    return jwt.encode({"sub": str(user_id), "username": username, "exp": expire}, JWT_SECRET, algorithm=JWT_ALGORITHM)

def _get_current_user(token: Optional[str] = Cookie(default=None)) -> Optional[dict]:
    if not token:
        return None
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return {"id": int(payload["sub"]), "username": payload["username"]}
    except JWTError:
        return None


# ── Auth endpoints ────────────────────────────────────────────────────────────

@app.post("/api/auth/register", status_code=201)
def register(body: RegisterBody, response: Response):
    if len(body.username) < 3 or len(body.password) < 6:
        raise HTTPException(400, "Username >= 3 chars, password >= 6 chars")
    neon = get_neon()
    existing = neon.execute(f"SELECT id FROM users WHERE username = '{body.username.replace(chr(39), chr(39)*2)}'")
    if existing:
        raise HTTPException(409, "Nom d'utilisateur déjà pris")
    hashed = _hash_password(body.password)
    rows = neon.execute(
        f"INSERT INTO users (username, password_hash) VALUES ('{body.username.replace(chr(39), chr(39)*2)}', '{hashed}') RETURNING id"
    )
    user_id = rows[0]["id"]
    token = _create_token(user_id, body.username)
    response.set_cookie("token", token, httponly=True, max_age=JWT_EXPIRE_DAYS * 86400, samesite="lax")
    return {"id": user_id, "username": body.username}


@app.post("/api/auth/login")
def login(body: LoginBody, response: Response):
    neon = get_neon()
    rows = neon.execute(f"SELECT id, password_hash FROM users WHERE username = '{body.username.replace(chr(39), chr(39)*2)}'")
    if not rows or not _verify_password(body.password, rows[0]["password_hash"]):
        raise HTTPException(401, "Identifiants incorrects")
    user_id = rows[0]["id"]
    token = _create_token(user_id, body.username)
    response.set_cookie("token", token, httponly=True, max_age=JWT_EXPIRE_DAYS * 86400, samesite="lax")
    return {"id": user_id, "username": body.username}


@app.post("/api/auth/logout")
def logout(response: Response):
    response.delete_cookie("token")
    return {"ok": True}


@app.get("/api/auth/me")
def me(token: Optional[str] = Cookie(default=None)):
    user = _get_current_user(token)
    if not user:
        raise HTTPException(401, "Non connecté")
    return user


# ── Filters ──────────────────────────────────────────────────────────────────

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


# ── Random release ────────────────────────────────────────────────────────────

@app.get("/api/random")
def get_random(
    popularity_min: int = Query(0, ge=0, le=100),
    popularity_max: int = Query(100, ge=0, le=100),
    year_min: Optional[int] = Query(None),
    year_max: Optional[int] = Query(None),
    genres: list[str] = Query(default=[]),
    exclude_genres: list[str] = Query(default=[]),
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
    if exclude_genres:
        list_sql = ", ".join(f"'{g.replace(chr(39), chr(39)*2)}'" for g in exclude_genres)
        conditions.append(f"NOT (genres && ARRAY[{list_sql}]::text[])")
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


# ── Cover proxy ───────────────────────────────────────────────────────────────

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


# ── Anonymous likes ───────────────────────────────────────────────────────────

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


# ── User likes ────────────────────────────────────────────────────────────────

@app.post("/api/user/like/{discogs_id}", status_code=201)
def add_user_like(discogs_id: int, token: Optional[str] = Cookie(default=None)):
    user = _get_current_user(token)
    if not user:
        raise HTTPException(401, "Non connecté")
    neon = get_neon()
    neon.execute(f"INSERT INTO user_likes (user_id, discogs_id) VALUES ({user['id']}, {discogs_id}) ON CONFLICT DO NOTHING")
    return {"liked": True}


@app.delete("/api/user/like/{discogs_id}")
def remove_user_like(discogs_id: int, token: Optional[str] = Cookie(default=None)):
    user = _get_current_user(token)
    if not user:
        raise HTTPException(401, "Non connecté")
    neon = get_neon()
    neon.execute(f"DELETE FROM user_likes WHERE user_id = {user['id']} AND discogs_id = {discogs_id}")
    return {"liked": False}


@app.get("/api/user/likes")
def get_user_likes(token: Optional[str] = Cookie(default=None)):
    user = _get_current_user(token)
    if not user:
        raise HTTPException(401, "Non connecté")
    neon = get_neon()
    return neon.execute(f"""
        SELECT r.discogs_id, r.title, r.artist, r.year, r.country,
               r.genres, r.styles, r.label, r.popularity_score,
               l.liked_at
        FROM user_likes l
        JOIN dim_releases r USING (discogs_id)
        WHERE l.user_id = {user['id']}
        ORDER BY l.liked_at DESC
    """)


# ── YouTube search ────────────────────────────────────────────────────────────

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


# ── Discogs community data ────────────────────────────────────────────────────

@app.get("/api/discogs/{discogs_id}")
async def get_discogs_community(discogs_id: int):
    headers = {
        "Authorization": f"Discogs token={DISCOGS_TOKEN}",
        "User-Agent": "GemDigger/0.1",
    }
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(f"{DISCOGS_BASE_URL}/releases/{discogs_id}", headers=headers)
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail="Discogs API error")
        data = resp.json()
    community = data.get("community", {})
    have = community.get("have", 0)
    want = community.get("want", 0)
    rarity = round(want / have, 4) if have > 0 else float(want)
    return {
        "community_have": have,
        "community_want": want,
        "rarity_score": rarity,
        "lowest_price": data.get("lowest_price"),
    }


# ── Static frontend ───────────────────────────────────────────────────────────

app.mount("/", StaticFiles(directory="static", html=True), name="static")
