#!/usr/bin/env bash
# =============================================================================
# create_raw_tables.sh
# Purpose:
#   Creates the raw_ticketing BigQuery dataset and all 5 raw source tables
#   used by the dbt Medallion Architecture pipeline.
#
#   Tables created:
#     - raw_ticketing.events
#     - raw_ticketing.customers
#     - raw_ticketing.tickets
#     - raw_ticketing.platforms
#     - raw_ticketing.channels
#
#   Table schemas are sourced from: scripts/schema/raw_ticketing/*.json
#   Each schema file contains full column definitions, descriptions,
#   partitioning, clustering, and labels aligned with dbt Silver/Gold models.
#
# Usage:
#   ./scripts/create_raw_tables.sh [OPTIONS]
#
# Options:
#   -p, --project     GCP project ID (required if PROJECT_ID env var not set)
#   -l, --location    BigQuery location (default: us-central1)
#   -r, --recreate    Drop and recreate tables if they already exist
#   -h, --help        Show this help message
#
# Examples:
#   # Use env vars
#   export PROJECT_ID=gen-lang-client-0194369893
#   export ENV=dev
#   ./scripts/create_raw_tables.sh
#
#   # Pass project inline (ENV still required as env var)
#   export ENV=prod
#   ./scripts/create_raw_tables.sh --project gen-lang-client-0194369893
#
#   # Recreate all tables (WARNING: drops existing data)
#   export ENV=dev
#   ./scripts/create_raw_tables.sh --project gen-lang-client-0194369893 --recreate
#
# Prerequisites:
#   - gcloud CLI installed and authenticated (gcloud auth login)
#   - bq CLI available (included with gcloud SDK)
#   - Sufficient IAM roles: bigquery.datasets.create, bigquery.tables.create
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
LOCATION="${BQ_LOCATION:-us-central1}"
RECREATE=false
SCHEMA_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/schema/raw_ticketing"
DATASET="raw_ticketing"

# ---------------------------------------------------------------------------
# Colours for output
# ---------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Colour

log_info()    { echo -e "${CYAN}[INFO]${NC}  $*"; }
log_success() { echo -e "${GREEN}[OK]${NC}    $*"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $*" >&2; }

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
usage() {
  grep '^#' "$0" | grep -v '#!/' | sed 's/^# \{0,1\}//'
  exit 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--project)  PROJECT_ID="$2"; shift 2 ;;
    -l|--location) LOCATION="$2";   shift 2 ;;
    -r|--recreate) RECREATE=true;   shift   ;;
    -h|--help)     usage ;;
    *) log_error "Unknown argument: $1"; exit 1 ;;
  esac
done

# ---------------------------------------------------------------------------
# Validate required inputs
# ---------------------------------------------------------------------------
if [[ -z "${PROJECT_ID:-}" ]]; then
  log_error "PROJECT_ID is not set. Use --project <id> or export PROJECT_ID=<id>"
  exit 1
fi

if [[ -z "${ENV:-}" ]]; then
  log_error "ENV is not set. Export it before running: export ENV=dev|staging|prod"
  exit 1
fi

if [[ ! -d "${SCHEMA_DIR}" ]]; then
  log_error "Schema directory not found: ${SCHEMA_DIR}"
  exit 1
fi

# List of tables to create (order: platforms first — referenced by channels FK)
TABLES=("platforms" "events" "customers" "channels" "tickets")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
dataset_exists() {
  bq --project_id="${PROJECT_ID}" show "${DATASET}" &>/dev/null
}

table_exists() {
  local table="$1"
  bq --project_id="${PROJECT_ID}" show "${DATASET}.${table}" &>/dev/null
}

# Substitute ${PROJECT_ID} and ${ENV} placeholders → writes rendered JSON to a temp file
render_schema() {
  local table="$1"
  local src="${SCHEMA_DIR}/${table}.json"
  local tmp
  tmp=$(mktemp "${TMPDIR:-/tmp}/schema_XXXXXX")
  sed \
    -e "s/\${PROJECT_ID}/${PROJECT_ID}/g" \
    -e "s/\${ENV}/${ENV}/g" \
    "${src}" > "${tmp}"
  echo "${tmp}"
}

# ---------------------------------------------------------------------------
# Step 1: Create dataset if not exists
# ---------------------------------------------------------------------------
echo ""
echo "============================================================"
echo "  Creating raw_ticketing tables in BigQuery"
echo "  Project  : ${PROJECT_ID}"
echo "  Dataset  : ${DATASET}"
echo "  Location : ${LOCATION}"
echo "  Env      : ${ENV}"
echo "  Recreate : ${RECREATE}"
echo "============================================================"
echo ""

if dataset_exists; then
  log_info "Dataset '${DATASET}' already exists — skipping creation."
else
  log_info "Creating dataset '${DATASET}' in location '${LOCATION}'..."
  bq --project_id="${PROJECT_ID}" mk \
    --dataset \
    --location="${LOCATION}" \
    --description="Raw ticketing data ingested from all platform sources. Not owned by dbt — consumed by dbt Silver models." \
    "${PROJECT_ID}:${DATASET}"
  log_success "Dataset '${DATASET}' created."
fi

# ---------------------------------------------------------------------------
# Step 2: Create each table
# ---------------------------------------------------------------------------
# NOTE: bq mk --table accepts ONLY a bare JSON array of field definitions as the
# schema file. The full table definition JSON (tableReference, labels, description,
# timePartitioning, clustering) must be decomposed and passed as individual CLI flags.
# ---------------------------------------------------------------------------
PASS=0
SKIP=0
FAIL=0

for TABLE in "${TABLES[@]}"; do
  SCHEMA_FILE="${SCHEMA_DIR}/${TABLE}.json"

  if [[ ! -f "${SCHEMA_FILE}" ]]; then
    log_warn "Schema file not found for table '${TABLE}': ${SCHEMA_FILE} — skipping."
    ((FAIL++)) || true
    continue
  fi

  echo ""
  log_info "Processing table: ${DATASET}.${TABLE}"

  # Handle existing tables
  if table_exists "${TABLE}"; then
    if [[ "${RECREATE}" == "true" ]]; then
      log_warn "Table '${TABLE}' exists — dropping for recreation (--recreate flag set)."
      bq --project_id="${PROJECT_ID}" rm -f --table "${DATASET}.${TABLE}"
    else
      log_warn "Table '${TABLE}' already exists — skipping. Use --recreate to overwrite."
      ((SKIP++)) || true
      continue
    fi
  fi

  # Render full JSON with variable substitution
  RENDERED=$(render_schema "${TABLE}")

  # 1. Extract schema.fields → bare array file required by bq mk
  FIELDS_FILE=$(mktemp "${TMPDIR:-/tmp}/fields_XXXXXX")
  python3 - <<PYEOF > "${FIELDS_FILE}"
import json
with open("${RENDERED}") as f:
    d = json.load(f)
print(json.dumps(d["schema"]["fields"], indent=2))
PYEOF

  # 2. Description
  TABLE_DESC=$(python3 - <<PYEOF
import json
with open("${RENDERED}") as f:
    d = json.load(f)
print(d.get("description", ""))
PYEOF
)

  # 3. Labels → --label key:value flags
  LABEL_FLAGS=()
  while IFS="=" read -r key val; do
    [[ -n "${key}" ]] && LABEL_FLAGS+=("--label=${key}:${val}")
  done < <(python3 - <<PYEOF
import json
with open("${RENDERED}") as f:
    d = json.load(f)
for k, v in d.get("labels", {}).items():
    print(f"{k}={v}")
PYEOF
)

  # 4. Partitioning flags
  PARTITION_FLAGS=()
  HAS_PARTITION=$(python3 - <<PYEOF
import json
with open("${RENDERED}") as f:
    d = json.load(f)
print("true" if "timePartitioning" in d else "false")
PYEOF
)
  if [[ "${HAS_PARTITION}" == "true" ]]; then
    PART_FIELD=$(python3 - <<PYEOF
import json
with open("${RENDERED}") as f:
    d = json.load(f)
print(d["timePartitioning"]["field"])
PYEOF
)
    PARTITION_FLAGS=(
      "--time_partitioning_type=DAY"
      "--time_partitioning_field=${PART_FIELD}"
    )
  fi

  # 5. Clustering flags
  CLUSTER_FLAGS=()
  HAS_CLUSTER=$(python3 - <<PYEOF
import json
with open("${RENDERED}") as f:
    d = json.load(f)
print("true" if "clustering" in d else "false")
PYEOF
)
  if [[ "${HAS_CLUSTER}" == "true" ]]; then
    CLUSTER_COLS=$(python3 - <<PYEOF
import json
with open("${RENDERED}") as f:
    d = json.load(f)
print(",".join(d["clustering"]["fields"]))
PYEOF
)
    CLUSTER_FLAGS=("--clustering_fields=${CLUSTER_COLS}")
  fi

  # Create the table — FIELDS_FILE is the bare JSON array bq mk expects
  bq --project_id="${PROJECT_ID}" mk \
    --table \
    --location="${LOCATION}" \
    --description="${TABLE_DESC}" \
    "${LABEL_FLAGS[@]+"${LABEL_FLAGS[@]}"}" \
    "${PARTITION_FLAGS[@]+"${PARTITION_FLAGS[@]}"}" \
    "${CLUSTER_FLAGS[@]+"${CLUSTER_FLAGS[@]}"}" \
    "${DATASET}.${TABLE}" \
    "${FIELDS_FILE}"

  # Clean up temp files
  rm -f "${RENDERED}" "${FIELDS_FILE}"

  log_success "Table '${DATASET}.${TABLE}' created successfully."
  ((PASS++)) || true
done

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "============================================================"
echo "  Done."
printf "  Created : %d\n" "${PASS}"
printf "  Skipped : %d\n" "${SKIP}"
printf "  Failed  : %d\n" "${FAIL}"
echo "============================================================"
echo ""

if [[ "${FAIL}" -gt 0 ]]; then
  exit 1
fi
