.PHONY: setup install db-start airflow-start download parse dbt-run full-ingest help

PYTHON  := python
DBT     := $(shell find ~/.local ~/.appdata /c/Users -name "dbt.exe" -o -name "dbt" 2>/dev/null | head -1)
ifeq ($(DBT),)
  DBT   := dbt
endif

help:
	@echo ""
	@echo "  Sampling Assisté — commandes disponibles"
	@echo ""
	@echo "  make setup          → installation complète (1ère fois)"
	@echo "  make install        → installe les dépendances Python"
	@echo "  make db-start       → démarre PostgreSQL (Docker)"
	@echo "  make airflow-start  → démarre Airflow (webserver + scheduler)"
	@echo "  make download       → télécharge le dernier dump Discogs"
	@echo "  make parse          → parse le dump et insère en base"
	@echo "  make dbt-run        → lance les transformations dbt"
	@echo "  make full-ingest    → download + parse + dbt"
	@echo ""

# ── Installation complète ─────────────────────────────────────────────────────

setup: install db-start airflow-start .env-check full-ingest
	@echo ""
	@echo "  Setup terminé. Lance 'python monitor.py' pour suivre l'ingestion."
	@echo ""

install:
	@echo "→ Installation des dépendances Python..."
	$(PYTHON) -m pip install -r requirements.txt
	$(PYTHON) -m pip install dbt-postgres
	@echo "→ Configuration du profil dbt..."
	$(PYTHON) scripts/setup_dbt_profile.py

.env-check:
	@if [ ! -f .env ]; then \
		echo ""; \
		echo "  ERREUR : fichier .env manquant."; \
		echo "  Crée un fichier .env avec :"; \
		echo "    DISCOGS_TOKEN=ton_token_ici"; \
		echo ""; \
		exit 1; \
	fi

# ── Services Docker ───────────────────────────────────────────────────────────

db-start:
	@echo "→ Démarrage de PostgreSQL..."
	docker-compose up -d postgres
	@echo "→ Attente que PostgreSQL soit prêt..."
	@until docker-compose exec postgres pg_isready -U postgres > /dev/null 2>&1; do sleep 1; done
	@echo "→ PostgreSQL prêt."

airflow-start:
	@echo "→ Démarrage d'Airflow (build + init + webserver + scheduler)..."
	docker-compose up -d --build airflow-init
	@echo "→ Attente de l'initialisation Airflow..."
	@until docker-compose ps airflow-init | grep -q "exited\|completed"; do sleep 2; done
	docker-compose up -d airflow-webserver airflow-scheduler
	@echo "→ Airflow disponible sur http://localhost:8080 (admin / admin)"

# ── Ingestion ─────────────────────────────────────────────────────────────────

download:
	@echo "→ Téléchargement du dernier dump Discogs..."
	$(PYTHON) -m ingestion.dump_downloader

parse:
	@echo "→ Parsing du dump et insertion en base..."
	$(PYTHON) -m ingestion.dump_parser

dbt-run:
	@echo "→ Transformations dbt..."
	cd sampling_dbt && $(DBT) run

full-ingest: download parse dbt-run
	@echo "→ Ingestion complète terminée."
