ARG AIRFLOW_VERSION=3.3.0
FROM apache/airflow:${AIRFLOW_VERSION}-python3.12

ARG AIRFLOW_VERSION

USER airflow
WORKDIR /opt/airflow/project

COPY --chown=airflow:root pyproject.toml ./
COPY --chown=airflow:root src ./src
COPY --chown=airflow:root dbt ./dbt

# Pin Airflow while installing the project's ingestion and dbt dependencies.
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    -e ".[dev]"