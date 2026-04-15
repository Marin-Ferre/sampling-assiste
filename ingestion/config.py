import os
from dotenv import load_dotenv

load_dotenv()

DISCOGS_TOKEN = os.environ["DISCOGS_TOKEN"]
DISCOGS_BASE_URL = "https://api.discogs.com"

# Genres cibles — à ajuster selon tes besoins
TARGET_GENRES = [
    "Blues",
    "Classical",
    "Electronic",
    "Folk, World, & Country",
    "Funk / Soul",
    "Hip Hop",
    "Jazz",
    "Latin",
    "Pop",
    "Reggae",
    "Rock",
    "Stage & Screen",
]

# PostgreSQL
PG_DSN = os.getenv(
    "PG_DSN",
    "postgresql://postgres:postgres@localhost:5432/sampling_assiste",
)

