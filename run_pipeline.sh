#!/usr/bin/env bash
# =============================================================================
# run_pipeline.sh
# Purpose: Orchestrates the full dbt Medallion pipeline for ticketing analytics.
# Usage:
#   ./run_pipeline.sh [dev|prod] [--full-refresh]
#
# Arguments:
#   dev|prod        — dbt target profile (default: dev)
#   --full-refresh  — Forces full refresh of all incremental models
#
# Pipeline order:
#   1. dbt deps       — Install/update packages
#   2. dbt seed       — Load currency_rates
#   3. dbt run        — Silver then Gold (enforced via tags)
#   4. dbt test       — All tests (schema + singular)
# =============================================================================

set -euo pipefail

TARGET=${1:-dev}
FULL_REFRESH=${2:-}

echo "=============================================="
echo " Ticketing Analytics dbt Pipeline"
echo " Target: ${TARGET}"
echo " Full Refresh: ${FULL_REFRESH:-no}"
echo "=============================================="

# 1. Install packages
echo ""
echo "[1/4] Installing dbt packages..."
dbt deps

# 2. Load seed data
echo ""
echo "[2/4] Loading seed data (currency_rates)..."
dbt seed --target "${TARGET}"

# 3. Run Silver layer
echo ""
echo "[3/4] Running Silver layer models..."
dbt run --select tag:silver --target "${TARGET}" ${FULL_REFRESH}

# 4. Run Gold layer
echo ""
echo "[3/4] Running Gold layer models..."
dbt run --select tag:gold --target "${TARGET}" ${FULL_REFRESH}

# 5. Run all tests
echo ""
echo "[4/4] Running all tests..."
dbt test --target "${TARGET}"

echo ""
echo "=============================================="
echo " Pipeline completed successfully!"
echo "=============================================="

# Optional: generate docs
if [ "${GENERATE_DOCS:-false}" = "true" ]; then
  echo ""
  echo "[Bonus] Generating dbt docs..."
  dbt docs generate --target "${TARGET}"
  dbt docs serve --port 8080
fi
