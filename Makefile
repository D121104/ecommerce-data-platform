COMPOSE ?= docker compose
PYTHON ?= python
AIRFLOW_SERVICE ?= airflow-api-server
DBT_DIR := /opt/airflow/project/dbt/ecommerce_dw

.DEFAULT_GOAL := help

.PHONY: help bootstrap install lint test test-all repository-check security-scan \
	compose-config db-up db-down db-reset status logs airflow-list airflow-trigger \
	monitor-trigger dbt-debug dbt-build dbt-snapshot dbt-run dbt-test validate

help:
	@echo "Available targets:"
	@echo "  bootstrap          Create a local .env with generated credentials"
	@echo "  install            Install the Python package and development tools"
	@echo "  lint               Run Ruff"
	@echo "  test               Run fast Python tests"
	@echo "  test-all           Run the full pytest suite"
	@echo "  repository-check   Validate repository files and DAG syntax"
	@echo "  security-scan      Scan tracked files for high-confidence secrets"
	@echo "  compose-config     Validate Docker Compose interpolation"
	@echo "  db-up              Start the complete local platform"
	@echo "  db-down            Stop containers and keep volumes"
	@echo "  db-reset           Stop containers and delete all local volumes"
	@echo "  status             Show container status"
	@echo "  logs               Follow recent platform logs"
	@echo "  airflow-list       List Airflow DAGs"
	@echo "  airflow-trigger    Trigger the daily pipeline DAG"
	@echo "  monitor-trigger    Trigger the monitoring DAG"
	@echo "  dbt-debug          Check the dbt connection inside Airflow"
	@echo "  dbt-build          Build and test dbt resources"
	@echo "  dbt-snapshot       Run dbt snapshots"
	@echo "  dbt-run            Run dbt models"
	@echo "  dbt-test           Run dbt data tests"
	@echo "  validate           Run local repository validation"

bootstrap:
	$(PYTHON) scripts/bootstrap.py

install:
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -e ".[dev]"

lint:
	ruff check src tests scripts

test:
	pytest -m "not integration"

test-all:
	pytest

repository-check:
	$(PYTHON) scripts/validate_repository.py

security-scan:
	$(PYTHON) scripts/security_scan.py

compose-config:
	$(COMPOSE) config --quiet

db-up: bootstrap
	$(COMPOSE) up -d --build --wait

db-down:
	$(COMPOSE) down

db-reset:
	@echo "WARNING: this deletes PostgreSQL, Airflow and Metabase volumes."
	$(COMPOSE) down -v

status:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=100

airflow-list:
	$(COMPOSE) exec $(AIRFLOW_SERVICE) airflow dags list

airflow-trigger:
	$(COMPOSE) exec $(AIRFLOW_SERVICE) airflow dags trigger ecommerce_daily_pipeline

monitor-trigger:
	$(COMPOSE) exec $(AIRFLOW_SERVICE) airflow dags trigger ecommerce_pipeline_monitor

dbt-debug:
	$(COMPOSE) exec $(AIRFLOW_SERVICE) bash -lc "cd $(DBT_DIR) && dbt debug --profiles-dir ."

dbt-build:
	$(COMPOSE) exec $(AIRFLOW_SERVICE) bash -lc "cd $(DBT_DIR) && dbt build --profiles-dir ."

dbt-snapshot:
	$(COMPOSE) exec $(AIRFLOW_SERVICE) bash -lc "cd $(DBT_DIR) && dbt snapshot --profiles-dir ."

dbt-run:
	$(COMPOSE) exec $(AIRFLOW_SERVICE) bash -lc "cd $(DBT_DIR) && dbt run --profiles-dir ."

dbt-test:
	$(COMPOSE) exec $(AIRFLOW_SERVICE) bash -lc "cd $(DBT_DIR) && dbt test --profiles-dir ."

validate: repository-check security-scan compose-config
