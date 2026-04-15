import os
from dotenv import load_dotenv

load_dotenv()

DISCOGS_TOKEN = os.environ["DISCOGS_TOKEN"]
DISCOGS_BASE_URL = "https://api.discogs.com"
RATE_LIMIT_DELAY = 1.1  # secondes entre requêtes (60 req/min)

# Genres cibles — à ajuster selon tes besoins
TARGET_GENRES = [
    "Jazz",
    "Electronic",
    "Hip Hop",
    "Funk / Soul",
    "Rock",
]

# PostgreSQL
PG_DSN = os.getenv(
    "PG_DSN",
    "postgresql://postgres:postgres@localhost:5432/sampling_assiste",
)

INGESTION_PAGE_SIZE = 100  # max autorisé par l'API Discogs
