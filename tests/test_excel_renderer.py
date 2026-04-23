# tests/test_excel_renderer.py
import sys
from pathlib import Path

from openpyxl import load_workbook

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.monthly_platform.excel_renderer import render_bundle_to_excel
from bundle_factory import make_test_bundle


def test_render_produces_all_expected_sheets(tmp_path):
    bundle = make_test_bundle()
    out = tmp_path / "test.xlsx"
    render_bundle_to_excel(bundle, out)
    wb = load_workbook(str(out))
    assert "Summary" in wb.sheetnames
    assert "Pipeline Open FY26" in wb.sheetnames
    assert "Won Lost FY26" in wb.sheetnames
    assert "Pipeline Inspection" in wb.sheetnames
    assert "Activity Volume" in wb.sheetnames


def test_summary_is_first_sheet(tmp_path):
    bundle = make_test_bundle()
    out = tmp_path / "test.xlsx"
    render_bundle_to_excel(bundle, out)
    wb = load_workbook(str(out))
    assert wb.sheetnames[0] == "Summary"


def test_pipeline_headers_match(tmp_path):
    bundle = make_test_bundle()
    out = tmp_path / "test.xlsx"
    render_bundle_to_excel(bundle, out)
    wb = load_workbook(str(out))
    ws = wb["Pipeline Open FY26"]
    headers = [ws.cell(1, c).value for c in range(1, 23)]
    assert headers[0] == "Account"
    assert headers[1] == "Opportunity"
    assert headers[6] == "ARR Unweighted (EUR)"
    assert headers[7] == "ARR Weighted (EUR)"
    assert headers[21] == "Competitor"


def test_pipeline_data_row(tmp_path):
    bundle = make_test_bundle()
    out = tmp_path / "test.xlsx"
    render_bundle_to_excel(bundle, out)
    wb = load_workbook(str(out))
    ws = wb["Pipeline Open FY26"]
    assert ws.cell(2, 1).value == "Acme Corp"
    assert ws.cell(2, 7).value == 500000.0
    assert ws.cell(2, 20).value == "Yes"


def test_freeze_panes_set(tmp_path):
    bundle = make_test_bundle()
    out = tmp_path / "test.xlsx"
    render_bundle_to_excel(bundle, out)
    wb = load_workbook(str(out))
    ws = wb["Pipeline Open FY26"]
    assert ws.freeze_panes == "A2"


def test_eur_formatting_applied(tmp_path):
    bundle = make_test_bundle()
    out = tmp_path / "test.xlsx"
    render_bundle_to_excel(bundle, out)
    wb = load_workbook(str(out))
    ws = wb["Pipeline Open FY26"]
    assert ws.cell(2, 7).number_format == "#,##0"


def test_empty_datasets_produce_no_rows(tmp_path):
    bundle = make_test_bundle()
    out = tmp_path / "test.xlsx"
    render_bundle_to_excel(bundle, out)
    wb = load_workbook(str(out))
    ws = wb["Renewals FY26"]
    assert ws.cell(1, 1).value is not None  # headers exist
    assert ws.cell(2, 1).value is None  # no data rows
