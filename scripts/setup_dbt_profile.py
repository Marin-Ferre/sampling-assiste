"""Configure le profil dbt dans ~/.dbt/profiles.yml."""

from pathlib import Path

PROFILES_DIR = Path.home() / ".dbt"
PROFILES_FILE = PROFILES_DIR / "profiles.yml"

PROFILE = """\
sampling_dbt:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: postgres
      password: postgres
      dbname: sampling_assiste
      schema: staging
      threads: 4
"""

PROFILES_DIR.mkdir(exist_ok=True)

if PROFILES_FILE.exists():
    content = PROFILES_FILE.read_text()
    if "sampling_dbt" in content:
        print(f"Profil dbt déjà configuré dans {PROFILES_FILE}")
    else:
        PROFILES_FILE.write_text(content + "\n" + PROFILE)
        print(f"Profil dbt ajouté dans {PROFILES_FILE}")
else:
    PROFILES_FILE.write_text(PROFILE)
    print(f"Profil dbt créé dans {PROFILES_FILE}")
