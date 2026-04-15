"""Wrapper minimaliste autour de l'API Discogs (user-token auth)."""

import time
import logging
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ingestion.config import DISCOGS_TOKEN, DISCOGS_BASE_URL, RATE_LIMIT_DELAY

logger = logging.getLogger(__name__)

_SESSION: requests.Session | None = None


def _get_session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        session = requests.Session()
        retry = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.headers.update(
            {
                "Authorization": f"Discogs token={DISCOGS_TOKEN}",
                "User-Agent": "SamplingAssiste/0.1 +https://github.com/mferre/sampling-assiste",
            }
        )
        _SESSION = session
    return _SESSION


def get(endpoint: str, params: dict[str, Any] | None = None) -> dict:
    """GET sur l'API Discogs avec rate-limiting et retries automatiques."""
    url = f"{DISCOGS_BASE_URL}{endpoint}"
    session = _get_session()

    response = session.get(url, params=params, timeout=30)

    # Respect du rate limit via le header Discogs
    remaining = int(response.headers.get("X-Discogs-Ratelimit-Remaining", 10))
    if remaining < 5:
        logger.warning("Rate limit proche (%d restants), pause de 60s", remaining)
        time.sleep(60)
    else:
        time.sleep(RATE_LIMIT_DELAY)

    response.raise_for_status()
    return response.json()
