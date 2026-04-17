"""
DAG de rafraîchissement dbt — quotidien à 6h UTC.

Reconstruit les modèles mart (dim_releases, fct_top_samples) pour
intégrer les nouvelles releases enrichies par community_enricher.
Tourne indépendamment du DAG d'ingestion.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "mferre",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

DBT_CMD = "cd /opt/airflow/sampling_dbt && dbt run --profiles-dir /opt/airflow/.dbt"

with DAG(
    dag_id="dbt_refresh",
    description="Rafraîchissement quotidien des modèles dbt (mart)",
    schedule="0 11 * * *",  # tous les jours à 11h UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["dbt", "transform"],
) as dag:

    BashOperator(
        task_id="dbt_run",
        bash_command=DBT_CMD,
        execution_timeout=timedelta(minutes=30),
    )
