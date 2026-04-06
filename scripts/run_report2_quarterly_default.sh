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
PHASE_SAQL_STATUS="skipped"
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
if [[ "$SKIP_SAQL_REFRESH" -eq 0 ]]; then
  if python3 "$SAQL_REFRESH" --snapshot-date "$SNAPSHOT_DATE" --target-org "$TARGET_ORG" --json > "$OUTPUT_DIR/saql_refresh_summary.json" 2> "$OUTPUT_DIR/saql_refresh_stderr.log"; then
    PHASE_SAQL_STATUS="ok"
  else
    PHASE_SAQL_STATUS="fail"
    echo "ERROR: SAQL refresh failed; see $OUTPUT_DIR/saql_refresh_stderr.log" >&2
    exit 2
  fi
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
SUMMARY_JSON=$(cat <<EOF
{
  "artifact_type": "report2_quarterly_run_summary",
  "run_date": "$RUN_DATE",
  "snapshot_date": "$SNAPSHOT_DATE",
  "target_org": "$TARGET_ORG",
  "output_dir": "$OUTPUT_DIR",
  "deck_path": "$DECK_OUT",
  "deck_summary_path": "$SUMMARY_OUT",
  "pdf_path": "$PDF_OUT",
  "montage_path": "$MONTAGE_OUT",
  "ql_thumb_path": "$QL_THUMB_OUT",
  "phases": {
    "preflight_pkill": "$PHASE_PKILL_STATUS",
    "auth": "$PHASE_AUTH_STATUS",
    "saql_refresh": "$PHASE_SAQL_STATUS",
    "deck_build": "$PHASE_BUILD_STATUS",
    "powerpoint_export": "$PHASE_EXPORT_STATUS",
    "powerpoint_export_retried": $EXPORT_RETRIED,
    "powerpoint_montage": "$PHASE_MONTAGE_STATUS",
    "ql_thumb": "$PHASE_THUMB_STATUS"
  }
}
EOF
)

echo "$SUMMARY_JSON" > "$OUTPUT_DIR/run_summary.json"

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
