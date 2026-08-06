from __future__ import annotations

import base64
import re
import secrets
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_EXAMPLE = PROJECT_ROOT / ".env.example"
ENV_FILE = PROJECT_ROOT / ".env"

PASSWORD_KEYS = (
    "POSTGRES_ADMIN_PASSWORD",
    "INGESTION_DB_PASSWORD",
    "DBT_DB_PASSWORD",
    "BI_DB_PASSWORD",
    "AIRFLOW_ADMIN_PASSWORD",
    "AIRFLOW_METADATA_DB_PASSWORD",
    "METABASE_APP_DB_PASSWORD",
)


def _fernet_key() -> str:
    """Generate a Fernet-compatible URL-safe key without exposing it."""

    return base64.urlsafe_b64encode(secrets.token_bytes(32)).decode("ascii")


def _replace_env_value(content: str, key: str, value: str) -> str:
    pattern = re.compile(rf"^{re.escape(key)}=.*$", re.MULTILINE)
    content, replacements = pattern.subn(f"{key}={value}", content, count=1)

    if replacements != 1:
        raise RuntimeError(f"Could not find {key} in {ENV_EXAMPLE.name}")

    return content


def main() -> int:
    if ENV_FILE.exists():
        print(f"{ENV_FILE.name} already exists; it was not modified.")
        return 0

    if not ENV_EXAMPLE.exists():
        raise FileNotFoundError(f"Missing template: {ENV_EXAMPLE}")

    content = ENV_EXAMPLE.read_text(encoding="utf-8")

    for key in PASSWORD_KEYS:
        content = _replace_env_value(
            content,
            key,
            secrets.token_urlsafe(32),
        )

    content = _replace_env_value(
        content,
        "AIRFLOW_FERNET_KEY",
        _fernet_key(),
    )
    content = _replace_env_value(
        content,
        "AIRFLOW_JWT_SECRET",
        secrets.token_urlsafe(48),
    )

    ENV_FILE.write_text(content, encoding="utf-8", newline="\n")

    try:
        ENV_FILE.chmod(0o600)
    except OSError:
        # Windows does not expose POSIX file modes consistently. The file is
        # still protected by .gitignore and is never printed by this script.
        pass

    print(f"Created {ENV_FILE.name} from {ENV_EXAMPLE.name}.")
    print("Generated local credentials and Airflow encryption secrets.")
    print("Review non-secret settings before running Docker Compose.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
