"""
DAG d'ingestion Discogs — quotidien.

Chaque jour :
  1. Vérifie si un nouveau dump mensuel est disponible
  2a. Si oui  → télécharge le dump → parse XML → dbt run
  2b. Si non  → ingestion API incrémentale (releases récentes) → dbt run
  3. Dans tous les cas → enrichissement community (have/want) par batch via API
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

DEFAULT_ARGS = {
    "owner": "mferre",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

DBT_CMD = "cd /opt/airflow/sampling_dbt && dbt run --profiles-dir /opt/airflow/.dbt"


# ---------------------------------------------------------------------------
# Branchement
# ---------------------------------------------------------------------------

def _check_new_dump(**context) -> str:
    """Retourne l'id de la tâche suivante selon qu'un nouveau dump est dispo."""
    from ingestion.dump_downloader import list_latest_dump, DUMPS_DIR
    _, filename = list_latest_dump()
    dest = DUMPS_DIR / filename
    if dest.exists():
        return "api_incremental"
    return "download_dump"


# ---------------------------------------------------------------------------
# Branche dump
# ---------------------------------------------------------------------------

def _download_dump_task(**context) -> str:
    from ingestion.dump_downloader import list_latest_dump, download, DUMPS_DIR
    url, filename = list_latest_dump()
    dest = DUMPS_DIR / filename
    download(url, dest)
    return str(dest)


def _parse_dump_task(**context) -> None:
    from ingestion.dump_parser import run
    ti = context["ti"]
    dump_path_str = ti.xcom_pull(task_ids="download_dump")
    run(Path(dump_path_str))


# ---------------------------------------------------------------------------
# Branche API incrémentale
# ---------------------------------------------------------------------------

def _api_incremental_task(**context) -> None:
    from ingestion.api_incremental import run
    run()


# ---------------------------------------------------------------------------
# Enrichissement community (have/want) — commun aux deux branches
# ---------------------------------------------------------------------------

def _community_enricher_task(**context) -> None:
    from ingestion.community_enricher import run
    # 50 000 releases par jour max = ~15h à 60 req/min
    run(batch_limit=50_000)


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

with DAG(
    dag_id="discogs_ingestion",
    description="Ingestion quotidienne Discogs (dump mensuel ou API incrémentale)",
    schedule="0 4 * * *",  # tous les jours à 4h UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ingestion", "discogs"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    check = BranchPythonOperator(
        task_id="check_new_dump",
        python_callable=_check_new_dump,
    )

    # Branche dump
    download = PythonOperator(
        task_id="download_dump",
        python_callable=_download_dump_task,
    )
    parse = PythonOperator(
        task_id="parse_and_insert",
        python_callable=_parse_dump_task,
        execution_timeout=timedelta(hours=12),
    )

    # Branche API
    api = PythonOperator(
        task_id="api_incremental",
        python_callable=_api_incremental_task,
        execution_timeout=timedelta(hours=3),
    )

    # dbt commun aux deux branches
    dbt = BashOperator(
        task_id="dbt_run",
        bash_command=DBT_CMD,
        execution_timeout=timedelta(hours=2),
        trigger_rule="none_failed_min_one_success",
    )

    # Enrichissement community — tourne après dbt, quelle que soit la branche
    enrich = PythonOperator(
        task_id="community_enricher",
        python_callable=_community_enricher_task,
        execution_timeout=timedelta(hours=16),
        trigger_rule="none_failed_min_one_success",
    )

    start >> check
    check >> download >> parse >> dbt
    check >> api >> dbt
    dbt >> enrich >> end
