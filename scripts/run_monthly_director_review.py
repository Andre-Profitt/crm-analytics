"""
Monthly Sales Directors Review, end-to-end ETL.

Steps, in sequence:
  1. Extract live Salesforce data into nine per-director workbooks.
  2. Render a Land-only deck for each director.
  3. Build the consolidated SharePoint analysis workbook.
  4. Build the dashboard and Q1 analysis workbook.
  5. Write a manifest.json listing every output file with row counts
     and deck slide counts so you can verify what ran.

Idempotent for a given run date. Re-running the same day overwrites that
day's outputs. Pass --date YYYY-MM-DD to backdate a run or recompute
a prior snapshot from the extract already on disk.

Skip flags let you rerun a single stage without redoing the rest:
  --skip-extract
  --skip-decks
  --skip-analysis

Exit code is 0 if every step succeeded, 1 otherwise.
"""

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from openpyxl import load_workbook


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_ROOT = ROOT / "output"
WORKBOOKS_ROOT = OUTPUT_ROOT / "director_live_workbooks"
DECKS_ROOT = OUTPUT_ROOT / "simcorp_director_decks"
SHAREPOINT_ROOT = OUTPUT_ROOT / "sharepoint"
LOGS_ROOT = OUTPUT_ROOT / "pipeline_logs"


def run_step(name, cmd, log_path):
    start = time.time()
    print(f"  [{name}] starting")
    with open(log_path, "w") as f:
        f.write(f"# {name}\n# {' '.join(str(c) for c in cmd)}\n\n")
        f.flush()
        result = subprocess.run(
            cmd,
            stdout=f,
            stderr=subprocess.STDOUT,
            cwd=ROOT,
            text=True,
        )
    duration = time.time() - start
    status = "ok" if result.returncode == 0 else "failed"
    print(f"  [{name}] {status} in {duration:.1f}s")
    return {
        "name": name,
        "command": " ".join(str(c) for c in cmd),
        "exit_code": result.returncode,
        "duration_seconds": round(duration, 1),
        "log_path": str(log_path.relative_to(ROOT)),
        "status": status,
    }


def inventory_xlsx(path):
    try:
        wb = load_workbook(path, data_only=True, read_only=True)
    except Exception as exc:
        return {"error": str(exc)}
    out = {}
    for sn in wb.sheetnames:
        ws = wb[sn]
        out[sn] = ws.max_row
    return out


def inventory_pptx(path):
    try:
        from pptx import Presentation

        prs = Presentation(str(path))
        return {"slides": len(prs.slides)}
    except Exception as exc:
        return {"error": str(exc)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date",
        default=datetime.now().strftime("%Y-%m-%d"),
        help="Run date stamp, YYYY-MM-DD. Defaults to today.",
    )
    parser.add_argument("--skip-extract", action="store_true")
    parser.add_argument("--skip-decks", action="store_true")
    parser.add_argument("--skip-analysis", action="store_true")
    args = parser.parse_args()

    date_stamp = args.date
    log_dir = LOGS_ROOT / date_stamp
    log_dir.mkdir(parents=True, exist_ok=True)

    print(f"Monthly Sales Directors Review pipeline, date {date_stamp}")
    print(f"Logs: {log_dir.relative_to(ROOT)}")
    print()

    manifest = {
        "run_date": date_stamp,
        "started_at": datetime.now().isoformat(timespec="seconds"),
        "steps": [],
        "outputs": {
            "extracts": [],
            "decks": [],
            "reports": [],
        },
    }

    # Stage 1: Extract live Salesforce data
    if not args.skip_extract:
        step = run_step(
            "1a_extract_salesforce",
            [sys.executable, "scripts/extract_director_live.py", "--all"],
            log_dir / "1a_extract_salesforce.log",
        )
        manifest["steps"].append(step)
        if step["exit_code"] != 0:
            print("Extract failed. Aborting downstream steps.")
            _write_manifest(manifest, log_dir)
            return 1

        # Historical Trending snapshots, appended to the per-director workbooks
        step = run_step(
            "1b_extract_historical_trending",
            [
                sys.executable,
                "scripts/extract_historical_trending.py",
                "--workbooks-dir",
                str(WORKBOOKS_ROOT / date_stamp),
            ],
            log_dir / "1b_extract_historical_trending.log",
        )
        manifest["steps"].append(step)

        # Data-quality audit — scans SF pipeline + account hygiene gaps,
        # writes per-run JSON + an Obsidian-ready summary + appends to the
        # rolling history ledger for month-over-month deltas. Runs regardless
        # of skip-analysis because data-quality is an extract-time concern.
        step = run_step(
            "1c_data_quality_audit",
            [
                sys.executable,
                "scripts/audit_data_quality.py",
                "--date",
                date_stamp,
            ],
            log_dir / "1c_data_quality_audit.log",
        )
        manifest["steps"].append(step)
    else:
        print("  skipping extract")

    # Stage 2: Analyze in Excel (SharePoint-ready analysis workbooks)
    if not args.skip_analysis:
        SHAREPOINT_ROOT.mkdir(parents=True, exist_ok=True)
        step = run_step(
            "2a_analyze_consolidated_review",
            [
                sys.executable,
                "scripts/build_sharepoint_analysis.py",
                "--workbooks-dir",
                str(WORKBOOKS_ROOT / date_stamp),
            ],
            log_dir / "2a_analyze_consolidated_review.log",
        )
        manifest["steps"].append(step)
        # Per-director regional workbooks: same tab structure as the master
        # but scoped to one territory. Directors receive their regional
        # workbook alongside their deck. Ops keeps the master above.
        sys.path.insert(0, str(ROOT / "scripts"))
        from build_sharepoint_analysis import DIRECTORS as _SD_DIRECTORS

        for _name, territory, _fname, *_ in _SD_DIRECTORS:
            safe = territory.replace(" ", "_").replace("&", "and").replace("/", "-")
            step = run_step(
                f"2a2_regional_{safe}",
                [
                    sys.executable,
                    "scripts/build_sharepoint_analysis.py",
                    "--workbooks-dir",
                    str(WORKBOOKS_ROOT / date_stamp),
                    "--territory",
                    territory,
                ],
                log_dir / f"2a2_regional_{safe}.log",
            )
            manifest["steps"].append(step)
        step = run_step(
            "2b_analyze_dashboard_q1",
            [sys.executable, "scripts/build_dashboard_analysis_excel.py"],
            log_dir / "2b_analyze_dashboard_q1.log",
        )
        manifest["steps"].append(step)
    else:
        print("  skipping analysis")

    workbooks_dir = WORKBOOKS_ROOT / date_stamp

    # Stage 3: Ship decks (the final deliverable)
    if not args.skip_decks:
        if not workbooks_dir.exists():
            print(f"  workbook directory missing: {workbooks_dir}")
        else:
            decks_dir = DECKS_ROOT / date_stamp / "land-only"
            decks_dir.mkdir(parents=True, exist_ok=True)
            for wb_path in sorted(workbooks_dir.glob("*.xlsx")):
                if wb_path.name.startswith("~"):
                    continue
                name = wb_path.stem
                out_path = decks_dir / f"{name}-LAND.pptx"
                step = run_step(
                    f"3_ship_deck_{name}",
                    [
                        sys.executable,
                        "scripts/build_deck_from_excel.py",
                        "--workbook",
                        str(wb_path),
                        "--output",
                        str(out_path),
                        "--land-only",
                    ],
                    log_dir / f"3_ship_deck_{name}.log",
                )
                manifest["steps"].append(step)
            # Executive rollup deck, one across all directors
            step = run_step(
                "3_ship_exec_rollup",
                [
                    sys.executable,
                    "scripts/build_exec_rollup_deck.py",
                    "--workbooks-dir",
                    str(workbooks_dir),
                    "--output",
                    str(decks_dir / "Exec Rollup.pptx"),
                ],
                log_dir / "3_ship_exec_rollup.log",
            )
            manifest["steps"].append(step)
    else:
        print("  skipping decks")

    # Inventory outputs for the manifest
    if workbooks_dir.exists():
        for wb_path in sorted(workbooks_dir.glob("*.xlsx")):
            if wb_path.name.startswith("~"):
                continue
            manifest["outputs"]["extracts"].append(
                {
                    "file": str(wb_path.relative_to(ROOT)),
                    "sheets": inventory_xlsx(wb_path),
                }
            )
    decks_dir = DECKS_ROOT / date_stamp / "land-only"
    if decks_dir.exists():
        for pptx in sorted(decks_dir.glob("*.pptx")):
            if pptx.name.startswith("~"):
                continue  # Office lockfiles from an open viewer
            manifest["outputs"]["decks"].append(
                {
                    "file": str(pptx.relative_to(ROOT)),
                    **inventory_pptx(pptx),
                }
            )
    for report_name in [
        "FY26 Pipeline Review, All Territories.xlsx",
        "Dashboard and Q1 Analysis.xlsx",
    ]:
        report_path = SHAREPOINT_ROOT / report_name
        if report_path.exists():
            manifest["outputs"]["reports"].append(
                {
                    "file": str(report_path.relative_to(ROOT)),
                    "sheets": inventory_xlsx(report_path),
                }
            )

    _write_manifest(manifest, log_dir)

    # Stage 4: Update Obsidian knowledge base
    if not args.skip_decks:
        step = run_step(
            "4_validate_tie_out",
            [
                sys.executable,
                "scripts/validate_tie_out.py",
                "--date",
                date_stamp,
            ],
            log_dir / "4_validate_tie_out.log",
        )
        manifest["steps"].append(step)
        _write_manifest(manifest, log_dir)

    if not args.skip_analysis and not args.skip_decks:
        step = run_step(
            "5_update_obsidian_notes",
            [
                sys.executable,
                "scripts/generate_obsidian_notes.py",
                "--date",
                date_stamp,
            ],
            log_dir / "5_update_obsidian_notes.log",
        )
        manifest["steps"].append(step)
        _write_manifest(manifest, log_dir)

    _print_summary(manifest)

    return 0 if all(s["exit_code"] == 0 for s in manifest["steps"]) else 1


def _write_manifest(manifest, log_dir):
    manifest["finished_at"] = datetime.now().isoformat(timespec="seconds")
    path = log_dir / "manifest.json"
    with open(path, "w") as f:
        json.dump(manifest, f, indent=2)


def _print_summary(manifest):
    print()
    print("=" * 70)
    print("Monthly Sales Directors Review, pipeline summary")
    print("=" * 70)
    print("Flow: Salesforce extract  ->  Excel analysis  ->  PowerPoint decks")
    print()
    for step in manifest["steps"]:
        print(
            f"  [{step['status']:6s}]  {step['name']:36s}"
            f"  {step['duration_seconds']:>6.1f}s"
        )
    print()
    run_date = manifest["run_date"]
    print("Outputs:")
    print(
        f"  Salesforce extracts:  {len(manifest['outputs']['extracts'])} workbooks"
        f"  (output/director_live_workbooks/{run_date}/)"
    )
    print(
        f"  Analysis reports:     {len(manifest['outputs']['reports'])} workbooks"
        f"  (output/sharepoint/)"
    )
    print(
        f"  PowerPoint decks:     {len(manifest['outputs']['decks'])} decks"
        f"  (output/simcorp_director_decks/{run_date}/land-only/)"
    )
    print()
    print(f"Manifest: output/pipeline_logs/{run_date}/manifest.json")
    if manifest["outputs"]["decks"]:
        print()
        print("Decks ready to ship:")
        for d in manifest["outputs"]["decks"]:
            print(f"  {d['file']}")


if __name__ == "__main__":
    sys.exit(main())
