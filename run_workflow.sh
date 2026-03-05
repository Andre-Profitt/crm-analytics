#!/bin/bash
# ============================================================================
# Book of Business - Full Workflow Runner (macOS)
# ============================================================================
# This script runs the Python extraction and report generation workflow.
#
# Note: Phase 1 (Power BI PDF export) must be done manually on macOS since
# Power Automate Desktop is Windows-only. Options:
#   - Export PDF manually from Power BI in browser
#   - Use a Windows VM with PAD
#   - Use browser automation (Playwright/Selenium) - see docs
# ============================================================================

set -e

echo ""
echo "============================================================"
echo "Book of Business - Automated Report Generation"
echo "============================================================"
echo ""

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Check for Python
if ! command -v python3 &> /dev/null; then
    echo "Error: python3 not found. Please install Python 3.9+."
    exit 1
fi

# Check for required packages
echo "Checking dependencies..."
python3 -c "import pdfplumber, pandas, openpyxl, pptx" 2>/dev/null || {
    echo "Installing missing dependencies..."
    pip3 install -r requirements.txt
}

# Check for PDF files
PDF_COUNT=$(ls -1 output/pdf/*.pdf 2>/dev/null | wc -l | tr -d ' ')
if [ "$PDF_COUNT" -eq 0 ]; then
    echo ""
    echo "WARNING: No PDF files found in output/pdf/"
    echo ""
    echo "To get started:"
    echo "  1. Open Power BI in your browser"
    echo "  2. Navigate to: Commercial Conversion Rates report"
    echo "  3. File → Export → PDF"
    echo "  4. Save as: output/pdf/ConversionRatesDashboard_$(date +%Y%m%d).pdf"
    echo ""
    read -p "Press Enter after placing the PDF, or Ctrl+C to cancel..."
fi

# Run the Python workflow
echo ""
echo "[Phase 2 + 3] Running Python extraction and report generation..."
echo ""

python3 src/main.py "$@"

if [ $? -eq 0 ]; then
    echo ""
    echo "============================================================"
    echo "SUCCESS! Workflow completed."
    echo "============================================================"
    echo ""
    echo "Output files:"
    echo "  - Excel: $SCRIPT_DIR/output/excel/ConversionRates_Master.xlsx"
    echo "  - PPTX:  $SCRIPT_DIR/output/pptx/"
    echo ""

    # Open output folder in Finder (optional)
    if [ "$1" != "--quiet" ] && [ "$1" != "-q" ]; then
        read -p "Open output folder in Finder? [y/N] " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            open "$SCRIPT_DIR/output"
        fi
    fi
else
    echo ""
    echo "============================================================"
    echo "ERROR: Workflow failed. Check the error messages above."
    echo "============================================================"
    exit 1
fi
