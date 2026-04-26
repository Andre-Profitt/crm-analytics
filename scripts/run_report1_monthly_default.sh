#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNNER="$ROOT_DIR/scripts/run_sales_director_monthly_report.py"

usage() {
  cat <<'EOF'
Usage:
  scripts/run_report1_monthly_default.sh <mode> [runner args...]

Modes:
  default   Run the main internal-review monthly deck flow. Defaults to --snapshot-date 2026-03-31.
  proof     Run the second-snapshot repeatability proof flow. Defaults to --snapshot-date 2026-02-28.
  publish   Run the publish-attempt flow. Defaults to --snapshot-date 2026-03-31.

Examples:
  scripts/run_report1_monthly_default.sh default --json
  scripts/run_report1_monthly_default.sh proof --output-dir output/sales_director_monthly_runs/proof_run --json
  scripts/run_report1_monthly_default.sh publish --finance-csv output/sales_director_monthly_runs/<run>/finance_churn_request.csv --commentary-csv output/sales_director_monthly_runs/<run>/owner_commentary_request.csv --json
EOF
}

if [[ $# -eq 0 ]]; then
  usage
  exit 1
fi

MODE="$1"
shift

case "$MODE" in
  -h|--help|help)
    usage
    exit 0
    ;;
esac

DEFAULT_SNAPSHOT=""
case "$MODE" in
  default)
    DEFAULT_SNAPSHOT="${REPORT1_SNAPSHOT_DATE:-2026-03-31}"
    ;;
  proof)
    DEFAULT_SNAPSHOT="${REPORT1_SNAPSHOT_DATE:-2026-02-28}"
    ;;
  publish)
    DEFAULT_SNAPSHOT="${REPORT1_SNAPSHOT_DATE:-2026-03-31}"
    ;;
  *)
    echo "Unknown mode: $MODE" >&2
    usage >&2
    exit 1
    ;;
esac

ARGS=("$@")
HAS_SNAPSHOT=0
for ((i = 0; i < ${#ARGS[@]}; i++)); do
  if [[ "${ARGS[$i]}" == "--snapshot-date" ]]; then
    HAS_SNAPSHOT=1
    break
  fi
done

CMD=(python3 "$RUNNER")
if [[ "$HAS_SNAPSHOT" -eq 0 ]]; then
  CMD+=(--snapshot-date "$DEFAULT_SNAPSHOT")
fi
CMD+=("${ARGS[@]}")

exec "${CMD[@]}"
