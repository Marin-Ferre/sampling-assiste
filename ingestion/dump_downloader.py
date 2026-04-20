"""
Télécharge le dernier dump mensuel Discogs releases dans data/dumps/.

Usage : python -m ingestion.dump_downloader
"""

import re
from pathlib import Path

import requests
from tqdm import tqdm

DUMPS_DIR = Path(__file__).parent.parent / "data" / "dumps"
ARCHIVE_DIR = DUMPS_DIR / "archive"
DISCOGS_DATA_URL = "https://data.discogs.com/"


def list_latest_dump() -> tuple[str, str]:
    """Retourne (url_download, filename) du dernier dump releases disponible."""
    from datetime import datetime
    year = datetime.now().year
    resp = requests.get(DISCOGS_DATA_URL, params={"prefix": f"data/{year}/"}, timeout=30)
    resp.raise_for_status()

    keys = re.findall(r'href="\?download=(data%2F\d+%2Fdiscogs_\d+_releases\.xml\.gz)"', resp.text)
    if not keys:
        raise RuntimeError(f"Aucun dump releases trouvé pour {year}")

    latest_key = sorted(keys)[-1]
    filename = latest_key.split("%2F")[-1]
    url = f"{DISCOGS_DATA_URL}?download={latest_key}"
    return url, filename


def download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)

    # Reprise partielle si le fichier existe déjà
    resume_pos = dest.stat().st_size if dest.exists() else 0
    headers = {"Range": f"bytes={resume_pos}-"} if resume_pos else {}

    with requests.get(url, headers=headers, stream=True, timeout=60) as r:
        if r.status_code == 416:
            print(f"Fichier déjà complet : {dest}")
            return
        r.raise_for_status()

        total = int(r.headers.get("Content-Length", 0)) + resume_pos
        mode = "ab" if resume_pos else "wb"

        with (
            open(dest, mode) as f,
            tqdm(
                total=total,
                initial=resume_pos,
                unit="B",
                unit_scale=True,
                desc=dest.name,
            ) as bar,
        ):
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
                bar.update(len(chunk))


def archive_previous(current: Path) -> None:
    """Archive l'ancien dump et supprime les archives plus anciennes."""
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)

    existing = sorted(DUMPS_DIR.glob("discogs_*_releases.xml.gz"))
    previous = [f for f in existing if f != current]

    for old in previous:
        dest = ARCHIVE_DIR / old.name
        old.rename(dest)
        print(f"Archivé : {dest}")

    # Garde uniquement le plus récent dans l'archive
    archived = sorted(ARCHIVE_DIR.glob("discogs_*_releases.xml.gz"))
    for obsolete in archived[:-1]:
        obsolete.unlink()
        print(f"Supprimé (obsolète) : {obsolete.name}")


def run() -> None:
    print("Recherche du dernier dump Discogs...")
    url, filename = list_latest_dump()
    dest = DUMPS_DIR / filename

    print(f"URL   : {url}")
    print(f"Dest  : {dest}")

    if dest.exists():
        print(f"Reprise depuis {dest.stat().st_size / 1e9:.2f} GB déjà téléchargés")

    download(url, dest)
    print(f"Téléchargement terminé : {dest}")

    archive_previous(dest)
    print("Archivage terminé.")


if __name__ == "__main__":
    run()
