"""
DAG de rafraîchissement dbt — quotidien à 8h UTC (10h Paris).

Reconstruit les modèles mart (dim_releases, fct_top_samples) pour
intégrer les nouvelles releases enrichies par community_enricher.
Tourne indépendamment du DAG d'ingestion.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

DEFAULT_ARGS = {
    "owner": "mferre",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

DBT_CMD = "cd /opt/airflow/sampling_dbt && dbt run --profiles-dir /opt/airflow/.dbt"
SYNC_CMD = "python /opt/airflow/scripts/sync_to_neon.py"

COUNT_BEFORE_CMD = """
psql postgresql://postgres:postgres@postgres:5432/sampling_assiste \
  -t -c "SELECT COUNT(*) FROM mart.dim_releases" | tr -d ' \n' > /tmp/count_before.txt
"""

NOTIFY_SUCCESS_CMD = """
COUNT=$(psql postgresql://postgres:postgres@postgres:5432/sampling_assiste \
  -t -c "SELECT COUNT(*) FROM mart.dim_releases" | tr -d ' \n')
BEFORE=$(cat /tmp/count_before.txt 2>/dev/null || echo 0)
ADDED=$((COUNT - BEFORE))
curl -s -o /dev/null \
  -H "Title: GemDigger OK" \
  -H "Priority: low" \
  -H "Tags: white_check_mark" \
  -d "${COUNT} releases (+${ADDED} cette session). Neon synchronise." \
  https://ntfy.sh/gemdigger-mferre
"""

def _notify_failure_cmd(task_name: str) -> str:
    return f"""
curl -s -o /dev/null \
  -H "Title: GemDigger ERREUR" \
  -H "Priority: high" \
  -H "Tags: rotating_light" \
  -d "Etape en echec : {task_name}. Verifiez Airflow." \
  https://ntfy.sh/gemdigger-mferre
"""

with DAG(
    dag_id="dbt_refresh",
    description="Rafraîchissement quotidien des modèles dbt (mart)",
    schedule="0 8 * * *",  # tous les jours à 8h UTC (10h Paris)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["dbt", "transform"],
) as dag:

    count_before = BashOperator(
        task_id="count_before",
        bash_command=COUNT_BEFORE_CMD,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=DBT_CMD,
        execution_timeout=timedelta(minutes=30),
    )

    dbt_fail = BashOperator(
        task_id="notify_dbt_fail",
        bash_command=_notify_failure_cmd("dbt_run"),
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    sync_neon = BashOperator(
        task_id="sync_neon",
        bash_command=SYNC_CMD,
        execution_timeout=timedelta(minutes=60),
    )

    sync_fail = BashOperator(
        task_id="notify_sync_fail",
        bash_command=_notify_failure_cmd("sync_neon"),
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    notify_success = BashOperator(
        task_id="notify_success",
        bash_command=NOTIFY_SUCCESS_CMD,
    )

    count_before >> dbt_run >> [sync_neon, dbt_fail]
    sync_neon >> [notify_success, sync_fail]
