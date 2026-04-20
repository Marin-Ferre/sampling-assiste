FROM apache/airflow:2.9.1

USER airflow

# Dépendances Python nécessaires aux DAGs
RUN pip install --no-cache-dir requests psycopg2-binary python-dotenv lxml tqdm dbt-postgres

# Copie du code d'ingestion, des DAGs et du projet dbt dans l'image
COPY --chown=airflow:root ingestion/ /opt/airflow/ingestion/
COPY --chown=airflow:root dags/ /opt/airflow/dags/
COPY --chown=airflow:root sampling_dbt/ /opt/airflow/sampling_dbt/

# Profile dbt pointant vers postgres (host réseau docker)
RUN mkdir -p /opt/airflow/.dbt && printf '%s\n' \
    'sampling_dbt:' \
    '  target: prod' \
    '  outputs:' \
    '    prod:' \
    '      type: postgres' \
    '      host: postgres' \
    '      port: 5432' \
    '      user: postgres' \
    '      password: postgres' \
    '      dbname: sampling_assiste' \
    '      schema: staging' \
    '      threads: 4' \
    > /opt/airflow/.dbt/profiles.yml
