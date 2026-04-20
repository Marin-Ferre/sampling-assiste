import os
from dotenv import load_dotenv

load_dotenv()

DISCOGS_TOKEN = os.environ["DISCOGS_TOKEN"]
DISCOGS_BASE_URL = "https://api.discogs.com"

# Genres cibles pour le sampling
# Note : "Afrobeat" est un style chez Discogs (sous Funk / Soul ou Jazz),
# pas un genre — il sera capturé via les genres parents ci-dessous.
TARGET_GENRES = [
    "Blues",
    "Funk / Soul",
    "Jazz",
    "Latin",
    "Reggae",
]

# Filtre sur les années (inclus)
YEAR_MIN = 1955
YEAR_MAX = 1990

# PostgreSQL
PG_DSN = os.getenv(
    "PG_DSN",
    "postgresql://postgres:postgres@localhost:5432/sampling_assiste",
)

