from __future__ import annotations

import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.run_sales_region_monthly_builder as module


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
    snapshot = {"component_books": [{"director_name": "Sarah Pittroff"}]}

    monkeypatch.setattr(module, "build_region_snapshot", lambda **kwargs: snapshot)
    monkeypatch.setattr(
        module,
        "build_validation_artifacts",
        lambda snapshot: {
            "validated_brief": "brief",
            "structured_fill_payload": {"slides": []},
            "powerpoint_build_prompt": "prompt",
        },
    )
    monkeypatch.setattr(module, "build_shell_deck", lambda **kwargs: {"deck_path": str(kwargs["output_path"]), "slide_count": 13})

    argv = [
        "run_sales_region_monthly_builder.py",
        "--snapshot-date",
        "2026-04-10",
        "--region-name",
        "EMEA",
        "--director-snapshot-root",
        str(tmp_path / "director"),
        "--region-snapshot-root",
        str(tmp_path / "region"),
        "--shell-root",
        str(tmp_path / "shells"),
        "--run-root",
        str(tmp_path / "runs"),
        "--allow-generated-shell-fallback",
        "--powerpoint-mode",
        "skip",
    ]
    monkeypatch.setattr(sys, "argv", argv)
    module.main()

    run_root = tmp_path / "runs" / "2026-04-10"
    manifests = list(run_root.glob("*/emea/manifest.json"))
    assert manifests
    manifest = json.loads(manifests[0].read_text())
    assert manifest["powerpoint_build"]["status"] == "skipped"
    assert manifest["shell_resolution"]["source"] == "generated"
    assert manifest["shell_resolution"]["publish_safe"] is False
    assert Path(manifest["powerpoint_fill_payload_path"]).exists()


def test_resolve_shell_path_prefers_canonical_when_available(tmp_path: Path) -> None:
    canonical_root = tmp_path / "canonical"
    generated_root = tmp_path / "generated"
    canonical_path = canonical_root / "2026-04-10" / "Sales Region Monthly Shell - EMEA.pptx"
    canonical_path.parent.mkdir(parents=True)
    canonical_path.write_bytes(b"pptx")

    shell_path, resolution = module.resolve_shell_path(
        shell_source="auto",
        region_name="EMEA",
        snapshot_date="2026-04-10",
        generated_shell_root=generated_root,
        canonical_shell_root=canonical_root,
        allow_generated_fallback=False,
    )

    assert shell_path == canonical_path
    assert resolution["source"] == "canonical"


def test_resolve_shell_path_requires_canonical_by_default(tmp_path: Path) -> None:
    canonical_root = tmp_path / "canonical"
    generated_root = tmp_path / "generated"

    try:
        module.resolve_shell_path(
            shell_source="canonical",
            region_name="EMEA",
            snapshot_date="2026-04-10",
            generated_shell_root=generated_root,
            canonical_shell_root=canonical_root,
            allow_generated_fallback=False,
        )
    except FileNotFoundError as exc:
        assert "Create and promote the canonical shell first" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected FileNotFoundError when canonical shell is missing.")


def test_resolve_shell_path_allows_explicit_generated_fallback(tmp_path: Path) -> None:
    canonical_root = tmp_path / "canonical"
    generated_root = tmp_path / "generated"

    shell_path, resolution = module.resolve_shell_path(
        shell_source="canonical",
        region_name="EMEA",
        snapshot_date="2026-04-10",
        generated_shell_root=generated_root,
        canonical_shell_root=canonical_root,
        allow_generated_fallback=True,
    )

    assert shell_path == generated_root / "2026-04-10" / "Sales Region Monthly Shell - EMEA.pptx"
    assert resolution["source"] == "generated"
    assert resolution["publish_safe"] is False
