#!/usr/bin/env bash
set -euo pipefail

# Empty scaffold initializer.
# Creates only directories and minimal __init__.py files.
# Does NOT create Alembic/SQLAlchemy/API/worker code.

TARGET_DIR="${1:-.}"

mkdir -p "${TARGET_DIR}"
cd "${TARGET_DIR}"

# Change this if you want a different package name
PKG_NAME="protea"

# Create src layout
mkdir -p "src/${PKG_NAME}"
mkdir -p "src/${PKG_NAME}/config/runs"

mkdir -p "src/${PKG_NAME}/core/operations"
mkdir -p "src/${PKG_NAME}/core/contracts"

mkdir -p "src/${PKG_NAME}/infrastructure/database"
mkdir -p "src/${PKG_NAME}/infrastructure/orm/models"
mkdir -p "src/${PKG_NAME}/infrastructure/queue"

mkdir -p "src/${PKG_NAME}/workers"
mkdir -p "src/${PKG_NAME}/api/routers"
mkdir -p "src/${PKG_NAME}/api/schemas"
mkdir -p "src/${PKG_NAME}/api/services"
mkdir -p "src/${PKG_NAME}/cli/commands"
mkdir -p "src/${PKG_NAME}/utils"

mkdir -p tests

# Minimal package markers (only if missing)
touch "src/${PKG_NAME}/__init__.py"
touch "src/${PKG_NAME}/config/__init__.py"
touch "src/${PKG_NAME}/core/__init__.py"
touch "src/${PKG_NAME}/core/operations/__init__.py"
touch "src/${PKG_NAME}/core/contracts/__init__.py"

touch "src/${PKG_NAME}/infrastructure/__init__.py"
touch "src/${PKG_NAME}/infrastructure/database/__init__.py"
touch "src/${PKG_NAME}/infrastructure/orm/__init__.py"
touch "src/${PKG_NAME}/infrastructure/orm/models/__init__.py"
touch "src/${PKG_NAME}/infrastructure/queue/__init__.py"

touch "src/${PKG_NAME}/workers/__init__.py"

touch "src/${PKG_NAME}/api/__init__.py"
touch "src/${PKG_NAME}/api/routers/__init__.py"
touch "src/${PKG_NAME}/api/schemas/__init__.py"
touch "src/${PKG_NAME}/api/services/__init__.py"

touch "src/${PKG_NAME}/cli/__init__.py"
touch "src/${PKG_NAME}/cli/commands/__init__.py"

touch "src/${PKG_NAME}/utils/__init__.py"

touch "tests/__init__.py"

# Optional: empty config placeholders (only if missing)
touch "src/${PKG_NAME}/config/system.yaml"
touch "src/${PKG_NAME}/config/constants.yaml"

# Optional: top-level placeholders (only if missing)
[[ -f README.md ]] || : > README.md
[[ -f .gitignore ]] || : > .gitignore

echo "OK: empty scaffold initialized at: $(pwd)"
echo "Package: src/${PKG_NAME}/"