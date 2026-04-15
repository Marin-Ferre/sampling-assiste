"""
Télécharge le dernier dump Discogs releases.

Usage : python -m ingestion.dump_downloader
Le fichier est sauvegardé dans data/dumps/
"""

import os
import re
import sys
import logging
from pathlib import Path

import requests
from tqdm import tqdm

logger = logging.getLogger(__name__)

DUMPS_DIR = Path(__file__).parent.parent / "data" / "dumps"
LISTING_URL = "https://data.discogs.com/?prefix=data/2025/"
BASE_URL = "https://data.discogs.com/data/2025/"


def get_latest_dump_filename() -> str:
    resp = requests.get(LISTING_URL, timeout=30)
    resp.raise_for_status()
    matches = re.findall(r"discogs_\d{8}_releases\.xml\.gz", resp.text)
    if not matches:
        raise RuntimeError("Aucun dump trouvé sur data.discogs.com")
    return sorted(set(matches))[-1]


def download(url: str, dest: Path) -> None:
    logger.info("Téléchargement : %s", url)
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        with (
            open(dest, "wb") as f,
            tqdm(
                total=total,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc=dest.name,
            ) as bar,
        ):
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
                bar.update(len(chunk))
    logger.info("Fichier sauvegardé : %s (%.1f MB)", dest, dest.stat().st_size / 1e6)


def run() -> Path:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s — %(message)s")
    DUMPS_DIR.mkdir(parents=True, exist_ok=True)

    filename = get_latest_dump_filename()
    dest = DUMPS_DIR / filename

    if dest.exists():
        logger.info("Dump déjà présent : %s", dest)
        return dest

    url = BASE_URL + filename
    download(url, dest)
    return dest


if __name__ == "__main__":
    path = run()
    print(f"\nDump disponible : {path}")
