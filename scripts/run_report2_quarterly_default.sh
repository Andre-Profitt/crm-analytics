#!/usr/bin/env bash
# Operator wrapper for Report 2 (Sales Ops Quarterly).
#
# Refreshes the Sales Ops SAQL inputs, builds the deck via pptxgenjs, exports
# to PDF via Microsoft PowerPoint, generates a montage, writes a Quick Look
# thumbnail, and prints a per-run summary.
#
# Mirrors the operator pattern of scripts/run_report1_monthly_default.sh.
#
# Usage:
#   scripts/run_report2_quarterly_default.sh default --snapshot-date 2026-04-01 --json

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKSPACE="$ROOT_DIR/output/sales_ops_quarterly_deck_2026-03-31"
BUILD_SCRIPT="$WORKSPACE/build_sales_ops_quarterly_deck.js"
SAQL_REFRESH="$ROOT_DIR/scripts/run_report2_saql_refresh.py"
EXPORT_HELPER="$ROOT_DIR/scripts/export_powerpoint_pdf.py"
# Note: render_slides.py and create_montage.py currently live under the
# Report 1 deck workspace. They are reusable PDF rasterization helpers,
# not Report-1-specific. Restructuring them into a canonical shared
# scripts/ location is tracked as a follow-up; for now we depend on the
# existing Report 1 path.
RENDER_SCRIPT="$ROOT_DIR/output/sales_director_monthly_deck_2026-03-31/scripts/render_slides.py"
MONTAGE_SCRIPT="$ROOT_DIR/output/sales_director_monthly_deck_2026-03-31/scripts/create_montage.py"
SLIDES_VENV_PY="$ROOT_DIR/.venv_slides/bin/python"
TARGET_ORG="${REPORT2_TARGET_ORG:-apro@simcorp.com}"

usage() {
  cat <<'EOF'
Usage:
  scripts/run_report2_quarterly_default.sh <mode> [options...]

Modes:
  default   Run the main Sales Ops quarterly deck flow.

Options:
  --snapshot-date YYYY-MM-DD   Defaults to today (UTC date).
  --output-dir PATH            Defaults to output/sales_ops_quarterly_runs/<runid>.
  --skip-saql-refresh          Skip the SAQL refresh phase (use existing JSONs as-is).
  --json                       Print a JSON summary at the end.

Examples:
  scripts/run_report2_quarterly_default.sh default --snapshot-date 2026-04-01 --json
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
  default)
    ;;
  *)
    echo "Unknown mode: $MODE" >&2
    usage >&2
    exit 1
    ;;
esac

SNAPSHOT_DATE="$(date -u +%Y-%m-%d)"
OUTPUT_DIR=""
SKIP_SAQL_REFRESH=0
WANT_JSON=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --snapshot-date)
      SNAPSHOT_DATE="$2"
      shift 2
      ;;
    --output-dir)
      OUTPUT_DIR="$2"
      shift 2
      ;;
    --skip-saql-refresh)
      SKIP_SAQL_REFRESH=1
      shift
      ;;
    --json)
      WANT_JSON=1
      shift
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if ! [[ "$SNAPSHOT_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "ERROR: --snapshot-date must be in YYYY-MM-DD format, got: $SNAPSHOT_DATE" >&2
  exit 1
fi

RUN_DATE="$(date -u +%Y-%m-%d)"
if [[ -z "$OUTPUT_DIR" ]]; then
  OUTPUT_DIR="$ROOT_DIR/output/sales_ops_quarterly_runs/${RUN_DATE}T_refresh_snapshot_${SNAPSHOT_DATE}"
fi
mkdir -p "$OUTPUT_DIR"
mkdir -p "$OUTPUT_DIR/powerpoint_review"
mkdir -p "$OUTPUT_DIR/ql_thumb"

DECK_OUT="$OUTPUT_DIR/sales_ops_quarterly_review_${SNAPSHOT_DATE}.pptx"
SUMMARY_OUT="$OUTPUT_DIR/sales_ops_quarterly_review_${SNAPSHOT_DATE}.summary.json"
PDF_OUT="$OUTPUT_DIR/powerpoint_review/sales_ops_quarterly_review_${SNAPSHOT_DATE}.pdf"
RENDERED_DIR="$OUTPUT_DIR/powerpoint_review/rendered"
MONTAGE_OUT="$OUTPUT_DIR/powerpoint_review/montage.png"
QL_THUMB_OUT="$OUTPUT_DIR/ql_thumb/sales_ops_quarterly_review_${SNAPSHOT_DATE}.pptx.png"

PHASE_PKILL_STATUS="ok"
PHASE_AUTH_STATUS="ok"
PHASE_SAQL_STATUS="pending"
PHASE_BUILD_STATUS="pending"
PHASE_EXPORT_STATUS="pending"
PHASE_MONTAGE_STATUS="pending"
PHASE_THUMB_STATUS="pending"
EXPORT_RETRIED=0

# --- Phase 0: preflight ---
pkill -9 'Microsoft PowerPoint' 2>/dev/null || true

if ! sf org display --target-org "$TARGET_ORG" --json > /dev/null 2>&1; then
  PHASE_AUTH_STATUS="fail"
  echo "ERROR: sf org display failed for $TARGET_ORG" >&2
  exit 1
fi

# --- Phase 1: SAQL refresh ---
if [[ "$SKIP_SAQL_REFRESH" -eq 1 ]]; then
  PHASE_SAQL_STATUS="skipped"
elif python3 "$SAQL_REFRESH" --snapshot-date "$SNAPSHOT_DATE" --target-org "$TARGET_ORG" --json > "$OUTPUT_DIR/saql_refresh_summary.json" 2> "$OUTPUT_DIR/saql_refresh_stderr.log"; then
  PHASE_SAQL_STATUS="ok"
else
  PHASE_SAQL_STATUS="fail"
  echo "ERROR: SAQL refresh failed; see $OUTPUT_DIR/saql_refresh_stderr.log" >&2
  exit 2
fi

# --- Phase 2: build deck ---
cd "$WORKSPACE"
if node "$BUILD_SCRIPT" \
     --output "$DECK_OUT" \
     --snapshot-date "$SNAPSHOT_DATE" \
     --deck-title "Quarterly Sales Ops Review" \
     --summary-json "$SUMMARY_OUT" \
     > "$OUTPUT_DIR/build_stdout.log" 2> "$OUTPUT_DIR/build_stderr.log"; then
  PHASE_BUILD_STATUS="ok"
else
  PHASE_BUILD_STATUS="fail"
  echo "ERROR: deck build failed; see $OUTPUT_DIR/build_stderr.log" >&2
  exit 3
fi
cd "$ROOT_DIR"

# --- Phase 3: PowerPoint PDF export with retry-once ---
do_export() {
  python3 "$EXPORT_HELPER" \
    --input "$DECK_OUT" \
    --output "$PDF_OUT" \
    --timeout-seconds 90 \
    --json \
    > "$OUTPUT_DIR/export_stdout.log" 2> "$OUTPUT_DIR/export_stderr.log"
}

if do_export; then
  PHASE_EXPORT_STATUS="ok"
else
  EXPORT_RETRIED=1
  pkill -9 'Microsoft PowerPoint' 2>/dev/null || true
  sleep 5
  if do_export; then
    PHASE_EXPORT_STATUS="ok_after_retry"
  else
    PHASE_EXPORT_STATUS="warn"
  fi
fi

# --- Phase 4: PDF -> page images -> montage (only if export succeeded) ---
if [[ "$PHASE_EXPORT_STATUS" == "ok" || "$PHASE_EXPORT_STATUS" == "ok_after_retry" ]]; then
  if "$SLIDES_VENV_PY" "$RENDER_SCRIPT" "$PDF_OUT" --output_dir "$RENDERED_DIR" \
       > "$OUTPUT_DIR/render_stdout.log" 2> "$OUTPUT_DIR/render_stderr.log"; then
    if PYTHONPATH="$ROOT_DIR/output/sales_director_monthly_deck_2026-03-31/scripts" \
         "$SLIDES_VENV_PY" "$MONTAGE_SCRIPT" --input_dir "$RENDERED_DIR" --output_file "$MONTAGE_OUT" \
         > "$OUTPUT_DIR/montage_stdout.log" 2> "$OUTPUT_DIR/montage_stderr.log"; then
      PHASE_MONTAGE_STATUS="ok"
    else
      PHASE_MONTAGE_STATUS="warn"
    fi
  else
    PHASE_MONTAGE_STATUS="warn"
  fi
else
  PHASE_MONTAGE_STATUS="skipped"
fi

# --- Phase 5: Quick Look thumbnail ---
if qlmanage -t -s 1200 -o "$OUTPUT_DIR/ql_thumb" "$DECK_OUT" \
     > "$OUTPUT_DIR/ql_thumb_stdout.log" 2> "$OUTPUT_DIR/ql_thumb_stderr.log"; then
  PHASE_THUMB_STATUS="ok"
else
  PHASE_THUMB_STATUS="warn"
fi

# --- Summary ---
# Build the summary JSON with python3 to safely escape any path or status
# value that might contain special characters. This avoids the heredoc
# injection vector where an OUTPUT_DIR or path with embedded quotes /
# backslashes / newlines would corrupt run_summary.json. We pass values
# via environment variables (NOT shell interpolation into the python
# source), so python sees them as plain strings and json.dumps escapes
# them correctly.
SUMMARY_PATH="$OUTPUT_DIR/run_summary.json"
RUN_DATE="$RUN_DATE" \
SNAPSHOT_DATE="$SNAPSHOT_DATE" \
TARGET_ORG="$TARGET_ORG" \
OUTPUT_DIR_VAL="$OUTPUT_DIR" \
DECK_OUT="$DECK_OUT" \
SUMMARY_OUT="$SUMMARY_OUT" \
PDF_OUT="$PDF_OUT" \
MONTAGE_OUT="$MONTAGE_OUT" \
QL_THUMB_OUT="$QL_THUMB_OUT" \
PHASE_PKILL_STATUS="$PHASE_PKILL_STATUS" \
PHASE_AUTH_STATUS="$PHASE_AUTH_STATUS" \
PHASE_SAQL_STATUS="$PHASE_SAQL_STATUS" \
PHASE_BUILD_STATUS="$PHASE_BUILD_STATUS" \
PHASE_EXPORT_STATUS="$PHASE_EXPORT_STATUS" \
PHASE_MONTAGE_STATUS="$PHASE_MONTAGE_STATUS" \
PHASE_THUMB_STATUS="$PHASE_THUMB_STATUS" \
EXPORT_RETRIED="$EXPORT_RETRIED" \
SUMMARY_PATH="$SUMMARY_PATH" \
python3 - <<'PYEOF'
import json
import os

payload = {
    "artifact_type": "report2_quarterly_run_summary",
    "run_date": os.environ["RUN_DATE"],
    "snapshot_date": os.environ["SNAPSHOT_DATE"],
    "target_org": os.environ["TARGET_ORG"],
    "output_dir": os.environ["OUTPUT_DIR_VAL"],
    "deck_path": os.environ["DECK_OUT"],
    "deck_summary_path": os.environ["SUMMARY_OUT"],
    "pdf_path": os.environ["PDF_OUT"],
    "montage_path": os.environ["MONTAGE_OUT"],
    "ql_thumb_path": os.environ["QL_THUMB_OUT"],
    "phases": {
        "preflight_pkill": os.environ["PHASE_PKILL_STATUS"],
        "auth": os.environ["PHASE_AUTH_STATUS"],
        "saql_refresh": os.environ["PHASE_SAQL_STATUS"],
        "deck_build": os.environ["PHASE_BUILD_STATUS"],
        "powerpoint_export": os.environ["PHASE_EXPORT_STATUS"],
        "powerpoint_export_retried": int(os.environ["EXPORT_RETRIED"]),
        "powerpoint_montage": os.environ["PHASE_MONTAGE_STATUS"],
        "ql_thumb": os.environ["PHASE_THUMB_STATUS"],
    },
}
out_path = os.environ["SUMMARY_PATH"]
with open(out_path, "w") as f:
    json.dump(payload, f, indent=2)
    f.write("\n")
PYEOF

SUMMARY_JSON=$(cat "$OUTPUT_DIR/run_summary.json")

if [[ "$WANT_JSON" -eq 1 ]]; then
  echo "$SUMMARY_JSON"
else
  echo "Run complete: $OUTPUT_DIR"
  echo "Deck:    $DECK_OUT"
  echo "PDF:     $PDF_OUT"
  echo "Montage: $MONTAGE_OUT"
  echo "Thumb:   $QL_THUMB_OUT"
  echo "Phases:  build=$PHASE_BUILD_STATUS export=$PHASE_EXPORT_STATUS montage=$PHASE_MONTAGE_STATUS thumb=$PHASE_THUMB_STATUS"
fi
