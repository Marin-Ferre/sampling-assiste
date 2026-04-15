"""
DAG d'ingestion Discogs → PostgreSQL.

Planification : quotidienne à 3h00 UTC
Structure :
  start
    └── ingest_<genre> (un task par genre, en parallèle)
          └── end
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from ingestion.fetch_releases import ingest_genre, init_db
from ingestion.config import TARGET_GENRES, PG_DSN

import psycopg2

DEFAULT_ARGS = {
    "owner": "mferre",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}


def _init_db_task() -> None:
    conn = psycopg2.connect(PG_DSN)
    try:
        init_db(conn)
    finally:
        conn.close()


def _ingest_genre_task(genre: str) -> None:
    conn = psycopg2.connect(PG_DSN)
    try:
        ingest_genre(conn, genre)
    finally:
        conn.close()


with DAG(
    dag_id="discogs_ingestion",
    description="Ingestion des releases Discogs par genre vers raw.releases",
    schedule="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ingestion", "discogs"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    init_task = PythonOperator(
        task_id="init_db",
        python_callable=_init_db_task,
    )

    genre_tasks = [
        PythonOperator(
            task_id=f"ingest_{genre.lower().replace(' / ', '_').replace(' ', '_')}",
            python_callable=_ingest_genre_task,
            op_kwargs={"genre": genre},
        )
        for genre in TARGET_GENRES
    ]

    start >> init_task >> genre_tasks >> end
