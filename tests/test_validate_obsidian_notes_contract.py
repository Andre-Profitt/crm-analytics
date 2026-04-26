import json
import sys
from pathlib import Path

from scripts import validate_obsidian_notes_contract as contract_script


def _seed_obsidian_outputs(root: Path, run_date: str) -> None:
    month_dir = root / "obsidian" / "Monthly" / run_date[:7]
    month_dir.mkdir(parents=True, exist_ok=True)
    (month_dir / "README.md").write_text("# README\n", encoding="utf-8")
    directors_dir = root / "obsidian" / "Directors"
    directors_dir.mkdir(parents=True, exist_ok=True)
    workbooks_dir = root / "output" / "director_live_workbooks" / run_date
    workbooks_dir.mkdir(parents=True, exist_ok=True)
    snapshot_directors = {}
    for director, territory, filename in contract_script.DIRECTORS:
        (workbooks_dir / filename).write_text("", encoding="utf-8")
        slug = contract_script._slug(director)
        (month_dir / f"{slug}.auto.md").write_text("# auto\n", encoding="utf-8")
        (month_dir / f"{slug}.notes.md").write_text("# notes\n", encoding="utf-8")
        (directors_dir / f"{slug}.md").write_text("# director\n", encoding="utf-8")
        snapshot_directors[director] = {"territory": territory}
    snapshot_history = {
        "snapshots": [
            {
                "run_date": run_date,
                "period": run_date[:7],
                "directors": snapshot_directors,
                "totals": {},
            }
        ]
    }
    (root / "obsidian" / "snapshot_history.json").write_text(
        json.dumps(snapshot_history, indent=2) + "\n",
        encoding="utf-8",
    )


def test_main_validates_obsidian_notes_contract(
    tmp_path: Path, monkeypatch
) -> None:
    _seed_obsidian_outputs(tmp_path, "2026-04-22")

    monkeypatch.setattr(contract_script, "ROOT", tmp_path)
    monkeypatch.setattr(contract_script, "VAULT", tmp_path / "obsidian")
    monkeypatch.setattr(
        contract_script,
        "WORKBOOKS_ROOT",
        tmp_path / "output" / "director_live_workbooks",
    )
    monkeypatch.setattr(
        contract_script,
        "OUTPUT_ROOT",
        tmp_path / "output" / "obsidian_notes_contract",
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "validate_obsidian_notes_contract.py",
            "--date",
            "2026-04-22",
        ],
    )

    assert contract_script.main() == 0
    audit_path = (
        tmp_path
        / "output"
        / "obsidian_notes_contract"
        / "2026-04-22"
        / "obsidian_notes_contract_audit.json"
    )
    payload = json.loads(audit_path.read_text(encoding="utf-8"))
    assert payload["status"] == "ok"
    assert payload["expected_director_count"] == len(contract_script.DIRECTORS)
    assert len(payload["validated"]) == len(contract_script.DIRECTORS)
    assert not payload["failures"]


def test_main_fails_when_auto_note_is_missing(
    tmp_path: Path, monkeypatch
) -> None:
    _seed_obsidian_outputs(tmp_path, "2026-04-22")
    month_dir = tmp_path / "obsidian" / "Monthly" / "2026-04"
    (month_dir / "jesper-tyrer.auto.md").unlink()

    monkeypatch.setattr(contract_script, "ROOT", tmp_path)
    monkeypatch.setattr(contract_script, "VAULT", tmp_path / "obsidian")
    monkeypatch.setattr(
        contract_script,
        "WORKBOOKS_ROOT",
        tmp_path / "output" / "director_live_workbooks",
    )
    monkeypatch.setattr(
        contract_script,
        "OUTPUT_ROOT",
        tmp_path / "output" / "obsidian_notes_contract",
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "validate_obsidian_notes_contract.py",
            "--date",
            "2026-04-22",
        ],
    )

    assert contract_script.main() == 1
    audit_path = (
        tmp_path
        / "output"
        / "obsidian_notes_contract"
        / "2026-04-22"
        / "obsidian_notes_contract_audit.json"
    )
    payload = json.loads(audit_path.read_text(encoding="utf-8"))
    assert payload["status"] == "failed"
    assert {
        "director": "Jesper Tyrer",
        "issue": "missing_auto_note",
        "message": f"missing {month_dir / 'jesper-tyrer.auto.md'}",
    } in payload["failures"]
