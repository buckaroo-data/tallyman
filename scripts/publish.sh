#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# Required env vars:
#
#   TWINE_USERNAME  — use the literal string __token__
#   TWINE_PASSWORD  — your PyPI API token (starts with pypi-)
#
# To get these:
#   1. Create an account at https://pypi.org/account/register/
#   2. Go to https://pypi.org/manage/account/token/ and create a token
#      scoped to "Entire account" (or just this project once it exists)
#   3. Export before running:
#        export TWINE_USERNAME=__token__
#        export TWINE_PASSWORD=pypi-<your token here>
# ---------------------------------------------------------------------------

missing=()

if [[ -z "${TWINE_USERNAME:-}" ]]; then
    missing+=("TWINE_USERNAME  (set to the literal string: __token__)")
fi

if [[ -z "${TWINE_PASSWORD:-}" ]]; then
    missing+=("TWINE_PASSWORD  (your PyPI API token, starts with pypi-)")
fi

if [[ ${#missing[@]} -gt 0 ]]; then
    echo "ERROR: required environment variables are not set:"
    for var in "${missing[@]}"; do
        echo "  $var"
    done
    echo ""
    echo "How to get a PyPI token:"
    echo "  1. Create an account at https://pypi.org/account/register/"
    echo "  2. Go to https://pypi.org/manage/account/token/"
    echo "  3. Create a token scoped to 'Entire account' (first publish)"
    echo "     or scoped to 'tallyman-data' (after the project exists)"
    echo ""
    echo "Then export before running:"
    echo "  export TWINE_USERNAME=__token__"
    echo "  export TWINE_PASSWORD=pypi-<your token here>"
    exit 1
fi

cd "$(dirname "$0")/.."

echo "--- cleaning old dist/ ---"
rm -rf dist/

echo "--- building ---"
uv build

echo "--- publishing to PyPI ---"
uv run twine upload dist/*

echo "--- done ---"
