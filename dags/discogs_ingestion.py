"""
DAG d'ingestion Discogs via Data Dumps mensuels.

Planification : 1er de chaque mois à 3h00 UTC
Structure :
  start
    └── download_dump       (télécharge le dernier dump releases)
    └── parse_and_insert    (parse XML + bulk insert PostgreSQL)
    └── end
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

DEFAULT_ARGS = {
    "owner": "mferre",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


def _download_dump_task(**context) -> str:
    from ingestion.dump_downloader import list_latest_dump, download, DUMPS_DIR
    from pathlib import Path
    url, filename = list_latest_dump()
    dest = DUMPS_DIR / filename
    download(url, dest)
    return str(dest)


def _parse_dump_task(**context) -> None:
    from ingestion.dump_parser import run
    ti = context["ti"]
    dump_path_str = ti.xcom_pull(task_ids="download_dump")
    run(Path(dump_path_str))


with DAG(
    dag_id="discogs_dump_ingestion",
    description="Ingestion mensuelle Discogs via Data Dumps (XML bulk)",
    schedule="0 3 1 * *",  # 1er du mois à 3h UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ingestion", "discogs", "dump"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    download = PythonOperator(
        task_id="download_dump",
        python_callable=_download_dump_task,
    )

    parse = PythonOperator(
        task_id="parse_and_insert",
        python_callable=_parse_dump_task,
        execution_timeout=timedelta(hours=12),
    )

    start >> download >> parse >> end
