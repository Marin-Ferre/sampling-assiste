FROM apache/airflow:2.9.1

USER airflow

# Dépendances Python nécessaires aux DAGs
RUN pip install --no-cache-dir requests psycopg2-binary python-dotenv

# Copie du code d'ingestion et des DAGs dans l'image
COPY --chown=airflow:root ingestion/ /opt/airflow/ingestion/
COPY --chown=airflow:root dags/ /opt/airflow/dags/
