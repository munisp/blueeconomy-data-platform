#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"
export PYTHONDONTWRITEBYTECODE=1

ruff format --check src tests scripts_validate.py
ruff check src tests scripts_validate.py
mypy src tests
pytest -q
bandit -q -r src
pip-audit -r requirements.lock
pip-audit -r requirements-dev.lock
python3 scripts_validate.py
rm -rf dist build
python3 -m build --no-isolation

git diff --check
printf '%s\n' 'Blue Economy data platform local verification completed successfully.'
