from __future__ import annotations

import ast
import re
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

REQUIRED_FILES = (
    ".env.example",
    ".gitignore",
    "docker-compose.yml",
    "Makefile",
    "README.md",
    "airflow/Dockerfile",
    "airflow/dags/ecommerce_daily_pipeline.py",
    "airflow/dags/ecommerce_pipeline_monitor.py",
    "dbt/ecommerce_dw/dbt_project.yml",
    "dbt/ecommerce_dw/profiles.yml",
    "sql/init/01_create_roles_and_schemas.sh",
    "sql/init/02_create_raw_tables.sh",
    "sql/init/03_create_monitoring_objects.sh",
    "sql/init/04_create_metabase_app_database.sh",
)

REQUIRED_ENV_KEYS = (
    "POSTGRES_ADMIN_PASSWORD",
    "INGESTION_DB_PASSWORD",
    "DBT_DB_PASSWORD",
    "BI_DB_PASSWORD",
    "AIRFLOW_ADMIN_PASSWORD",
    "AIRFLOW_METADATA_DB_PASSWORD",
    "AIRFLOW_FERNET_KEY",
    "AIRFLOW_JWT_SECRET",
    "METABASE_APP_DB_PASSWORD",
    "DBT_TARGET",
    "DBT_SCHEMA",
    "MONITOR_EXPECTED_RUN_HOUR_UTC",
    "MONITOR_EXPECTED_RUN_GRACE_MINUTES",
)

DAG_FILES = (
    "airflow/dags/ecommerce_daily_pipeline.py",
    "airflow/dags/ecommerce_pipeline_monitor.py",
)


def _tracked_files() -> set[str]:
    result = subprocess.run(
        ["git", "ls-files"],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return set(result.stdout.splitlines())


def _read(relative_path: str) -> str:
    return (PROJECT_ROOT / relative_path).read_text(encoding="utf-8")


def main() -> int:
    errors: list[str] = []

    for relative_path in REQUIRED_FILES:
        if not (PROJECT_ROOT / relative_path).is_file():
            errors.append(f"missing required file: {relative_path}")

    try:
        tracked = _tracked_files()
    except (OSError, subprocess.CalledProcessError) as exc:
        errors.append(f"could not inspect Git index: {exc}")
        tracked = set()

    forbidden_tracked = {
        ".env",
        "docker-compose.backup.yml",
        "airflow/simple_auth_manager_passwords.json",
    }
    for relative_path in sorted(forbidden_tracked & tracked):
        errors.append(f"sensitive or backup file is tracked: {relative_path}")

    if (PROJECT_ROOT / ".env").exists():
        print("Local .env exists; it is intentionally ignored and was not inspected.")

    env_example = _read(".env.example")
    env_keys = {
        match.group(1)
        for match in re.finditer(r"^([A-Z][A-Z0-9_]*)=", env_example, re.MULTILINE)
    }
    for key in REQUIRED_ENV_KEYS:
        if key not in env_keys:
            errors.append(f".env.example is missing {key}")

    compose = _read("docker-compose.yml")
    required_compose_fragments = (
        "airflow_auth:/opt/airflow/auth",
        "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE",
        "MONITOR_EXPECTED_RUN_HOUR_UTC",
        "MONITOR_EXPECTED_RUN_GRACE_MINUTES",
    )
    for fragment in required_compose_fragments:
        if fragment not in compose:
            errors.append(f"docker-compose.yml is missing expected fragment: {fragment}")

    readme = _read("README.md")
    stale_readme_phrases = (
        "Project initialization",
        "Architecture documentation will be added",
    )
    for phrase in stale_readme_phrases:
        if phrase in readme:
            errors.append(f"README still contains stale placeholder text: {phrase}")

    makefile = _read("Makefile")
    for target in ("bootstrap", "db-up", "db-reset", "dbt-build", "validate"):
        if not re.search(rf"^{re.escape(target)}:", makefile, re.MULTILINE):
            errors.append(f"Makefile is missing target: {target}")

    for relative_path in DAG_FILES:
        try:
            ast.parse(_read(relative_path), filename=relative_path)
        except SyntaxError as exc:
            errors.append(f"DAG syntax error in {relative_path}: {exc}")

    if errors:
        print("Repository validation failed:")
        for error in errors:
            print(f"- {error}")
        return 1

    print("Repository validation passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
