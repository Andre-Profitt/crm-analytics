#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PY="$ROOT/.venv_slides/bin/python"
RENDER_SCRIPT="$ROOT/output/sales_director_monthly_deck_2026-03-31/scripts/render_slides.py"
MONTAGE_SCRIPT="$ROOT/output/sales_director_monthly_deck_2026-03-31/scripts/create_montage.py"
TEST_SCRIPT="$ROOT/output/sales_director_monthly_deck_2026-03-31/scripts/slides_test.py"
FONT_SCRIPT="$ROOT/output/sales_director_monthly_deck_2026-03-31/scripts/detect_font.py"
SCRIPT_DIR="$(dirname "$RENDER_SCRIPT")"

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <deck.pptx> [output_dir]" >&2
  exit 2
fi

if [[ ! -x "$VENV_PY" ]]; then
  echo "Missing validation venv at $VENV_PY" >&2
  echo "Create it with: python3 -m venv .venv_slides && .venv_slides/bin/python -m pip install pdf2image numpy python-pptx" >&2
  exit 1
fi

if ! command -v soffice >/dev/null 2>&1; then
  echo "Missing soffice on PATH. Install LibreOffice first." >&2
  exit 1
fi

DECK_PATH="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
if [[ ! -f "$DECK_PATH" ]]; then
  echo "Deck not found: $DECK_PATH" >&2
  exit 1
fi

OUT_DIR="${2:-}"
if [[ -z "$OUT_DIR" ]]; then
  deck_dir="$(dirname "$DECK_PATH")"
  deck_base="$(basename "$DECK_PATH" .pptx)"
  OUT_DIR="$deck_dir/${deck_base}_validation"
fi

mkdir -p "$OUT_DIR/rendered"

echo "Rendering slides to $OUT_DIR/rendered"
PYTHONPATH="$SCRIPT_DIR${PYTHONPATH:+:$PYTHONPATH}" \
  "$VENV_PY" "$RENDER_SCRIPT" "$DECK_PATH" --output_dir "$OUT_DIR/rendered"

echo "Building montage"
PYTHONPATH="$SCRIPT_DIR${PYTHONPATH:+:$PYTHONPATH}" \
  "$VENV_PY" "$MONTAGE_SCRIPT" --input_dir "$OUT_DIR/rendered" --output_file "$OUT_DIR/montage.png"

echo "Running overflow check"
PYTHONPATH="$SCRIPT_DIR${PYTHONPATH:+:$PYTHONPATH}" \
  "$VENV_PY" "$TEST_SCRIPT" "$DECK_PATH"

echo "Checking font substitution"
PYTHONPATH="$SCRIPT_DIR${PYTHONPATH:+:$PYTHONPATH}" \
  "$VENV_PY" "$FONT_SCRIPT" "$DECK_PATH" --json > "$OUT_DIR/font_report.json"

echo "Validation complete:"
echo "  rendered: $OUT_DIR/rendered"
echo "  montage:  $OUT_DIR/montage.png"
echo "  fonts:    $OUT_DIR/font_report.json"
