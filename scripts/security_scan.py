from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

TEXT_EXTENSIONS = {
    ".cfg",
    ".conf",
    ".env",
    ".example",
    ".github",
    ".ini",
    ".json",
    ".md",
    ".py",
    ".sql",
    ".toml",
    ".txt",
    ".yml",
    ".yaml",
}

IGNORED_PARTS = {
    ".git",
    ".pytest_cache",
    ".ruff_cache",
    "__pycache__",
    "dbt_packages",
    "logs",
    "target",
}

SECRET_PATTERNS = (
    re.compile(r"-----BEGIN (?:RSA|OPENSSH|EC|DSA|PRIVATE) KEY-----"),
    re.compile(r"(?i)\bgh[pousr]_[A-Za-z0-9_]{20,}\b"),
    re.compile(r"(?i)\bsk-[A-Za-z0-9]{20,}\b"),
    re.compile(r"(?i)\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"(?i)authorization\s*:\s*bearer\s+[A-Za-z0-9._~+/=-]{20,}"),
)

ASSIGNMENT_PATTERN = re.compile(
    r"(?i)\b(api[_-]?key|access[_-]?token|private[_ -]?key)\s*[:=]\s*['\"]?([^\s'\"${}]+)"
)

PLACEHOLDERS = {
    "change_me",
    "change-me",
    "example",
    "placeholder",
    "replace_with_generated_fernet_key",
    "replace_with_long_random_jwt_secret",
    "your_api_key",
    "your_token",
}


def _tracked_files() -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files"],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return [PROJECT_ROOT / item for item in result.stdout.splitlines()]


def _is_text_file(path: Path) -> bool:
    return path.suffix.lower() in TEXT_EXTENSIONS or path.name in {"Makefile", ".gitignore"}


def _is_placeholder(value: str) -> bool:
    normalized = value.strip().lower()
    return normalized in PLACEHOLDERS or any(
        marker in normalized
        for marker in ("${", "<your", "replace_me", "replace-me")
    )


def main() -> int:
    findings: list[tuple[str, int, str]] = []

    try:
        files = _tracked_files()
    except (OSError, subprocess.CalledProcessError) as exc:
        print(f"Unable to inspect Git index: {exc}", file=sys.stderr)
        return 2

    for path in files:
        if not path.is_file() or not _is_text_file(path):
            continue
        if any(part in IGNORED_PARTS for part in path.parts):
            continue

        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except UnicodeDecodeError:
            continue

        for line_number, line in enumerate(lines, start=1):
            if any(pattern.search(line) for pattern in SECRET_PATTERNS):
                findings.append(
                    (
                        str(path.relative_to(PROJECT_ROOT)),
                        line_number,
                        "high-confidence secret pattern",
                    )
                )
                continue

            assignment = ASSIGNMENT_PATTERN.search(line)
            if assignment and not _is_placeholder(assignment.group(2)):
                findings.append(
                    (
                        str(path.relative_to(PROJECT_ROOT)),
                        line_number,
                        "credential assignment requires review",
                    )
                )

    if findings:
        print("Potential secrets found in tracked files:")
        for relative_path, line_number, reason in findings:
            print(f"- {relative_path}:{line_number}: {reason}")
        return 1

    print("Security scan passed: no high-confidence secrets found in tracked files.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
