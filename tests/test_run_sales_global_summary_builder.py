from __future__ import annotations

import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.run_sales_global_summary_builder as module


def test_run_powerpoint_build_copies_shell_and_uses_powerpoint(monkeypatch, tmp_path: Path) -> None:
    shell = tmp_path / "shell.pptx"
    shell.write_bytes(b"pptx")
    captured: dict[str, object] = {}

    def fake_run_skill(target, **kwargs):  # type: ignore[no-untyped-def]
        captured["target"] = target.key
        captured.update(kwargs)
        return {"message_copied": False}

    monkeypatch.setattr(module, "run_skill", fake_run_skill)

    result = module.run_powerpoint_build(
        shell_path=shell,
        prompt_text="prompt",
        run_dir=tmp_path / "run" / "powerpoint_build",
        timeout=120,
    )

    assert captured["target"] == "powerpoint"
    assert captured["edit_permission_mode"] == "always-allow"
    assert captured["save_document_on_finish"] is True
    assert "[build " in str(captured["source_file"])
    assert result["status"] == "ok"


def test_main_skips_powerpoint_when_requested(monkeypatch, tmp_path: Path) -> None:
    snapshot = {
        "regions": [{"region_name": "APAC"}, {"region_name": "EMEA"}, {"region_name": "North America"}],
    }

    monkeypatch.setattr(module, "build_global_summary_snapshot", lambda **kwargs: snapshot)
    monkeypatch.setattr(
        module,
        "build_validation_artifacts",
        lambda snapshot: {
            "validated_brief": "brief",
            "structured_fill_payload": {"slides": []},
            "powerpoint_build_prompt": "prompt",
        },
    )
    monkeypatch.setattr(
        module,
        "build_deterministic_preview",
        lambda **kwargs: {"status": "ok", "deck_path": str(tmp_path / "baseline.pptx")},
    )
    monkeypatch.setattr(
        module,
        "render_deterministic_preview",
        lambda **kwargs: {"status": "ok", "montage_path": str(tmp_path / "montage.png"), "font_report": {}},
    )
    monkeypatch.setattr(
        module,
        "build_deterministic_preview_audit",
        lambda **kwargs: {"status": "ok", "ok": True, "finding_count": 0, "findings": []},
    )
    canonical_root = tmp_path / "canonical" / "2026-04-10"
    canonical_root.mkdir(parents=True)
    (canonical_root / "Sales Global Summary Shell.pptx").write_bytes(b"pptx")

    argv = [
        "run_sales_global_summary_builder.py",
        "--snapshot-date",
        "2026-04-10",
        "--director-snapshot-root",
        str(tmp_path / "director"),
        "--region-snapshot-root",
        str(tmp_path / "region"),
        "--global-snapshot-root",
        str(tmp_path / "global"),
        "--canonical-shell-root",
        str(tmp_path / "canonical"),
        "--fallback-shell-root",
        str(tmp_path / "fallback"),
        "--run-root",
        str(tmp_path / "runs"),
        "--powerpoint-mode",
        "skip",
    ]
    monkeypatch.setattr(sys, "argv", argv)
    module.main()

    run_root = tmp_path / "runs" / "2026-04-10"
    manifests = list(run_root.glob("*/manifest.json"))
    assert manifests
    manifest = json.loads(manifests[0].read_text())
    assert manifest["powerpoint_build"]["status"] == "skipped"
    assert manifest["shell_resolution"]["source"] == "canonical"
    assert Path(manifest["powerpoint_fill_payload_path"]).exists()
    assert manifest["deterministic_preview"]["status"] == "ok"


def test_resolve_shell_path_requires_canonical_by_default(tmp_path: Path) -> None:
    try:
        module.resolve_shell_path(
            snapshot_date="2026-04-10",
            canonical_shell_root=tmp_path / "canonical",
            fallback_shell_root=tmp_path / "fallback",
            allow_generated_fallback=False,
        )
    except FileNotFoundError as exc:
        assert "Create and promote the canonical shell first" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected FileNotFoundError when canonical shell is missing.")


def test_resolve_shell_path_allows_explicit_generated_fallback(tmp_path: Path) -> None:
    fallback = tmp_path / "fallback" / "2026-04-10"
    fallback.mkdir(parents=True)
    shell = fallback / "Sales Global Summary Shell.pptx"
    shell.write_bytes(b"pptx")

    shell_path, resolution = module.resolve_shell_path(
        snapshot_date="2026-04-10",
        canonical_shell_root=tmp_path / "canonical",
        fallback_shell_root=tmp_path / "fallback",
        allow_generated_fallback=True,
    )

    assert shell_path == shell
    assert resolution["source"] == "generated"
    assert resolution["publish_safe"] is False
