# Track G-Lite — release packet orchestrator (results)

Branch: `integration/track-f-template-first-builder`
Predecessor: render-gate tightening commit `6abd5e7` (all 8 validators clean)

## What this is

Track G-Lite is the orchestration layer that ties every Track E + Track F validator into a **single release-evidence artifact**. It runs all 8 validators end-to-end against the live anchors, captures each report, computes content digests of every input artifact, and emits a `release_packet.json` (plus a Markdown summary) with a single `publish_decision` field.

## What this is NOT

This is **not the full Track G** GPT specified. The full Track G includes upstream ETL, warehouse build, source-quality baselines, distribution comparison, semantic narrative, and SharePoint upload — all of which depend on inputs that aren't present in isolated sessions. Track G-Lite covers the contract+validator orchestration layer; the full Track G builds on it.

## Acceptance gate

| Criterion                                                                            | Status   |
| ------------------------------------------------------------------------------------ | -------- |
| All 8 validators run end-to-end via a single CLI                                     | **pass** |
| Aggregate decision logic emits `publish_ready` / `blocked_with_warnings` / `blocked` | **pass** |
| Input artifacts get content-digested for provenance                                  | **pass** |
| Mutated contract triggers `blocked` (negative control)                               | **pass** |
| `--skip-visual` records as `skipped`, not `failed`                                   | **pass** |

## Live state

```
release_packet: publish_decision=publish_ready (8/8 pass, 0 blockers, 0 warnings)

  deck_contract                  pass  blockers=0 warnings=0
  director_workbook_contract     pass  blockers=0 warnings=0
  director_workbook_validation   pass  blockers=0 warnings=0
  deck_bindings                  pass  blockers=0 warnings=0
  pptx_contract                  pass  blockers=0 warnings=0
  brand_fingerprint              pass  blockers=0 warnings=0
  deck_render                    pass  blockers=0 warnings=0
  deck_visual_regression         pass  blockers=0 warnings=0
```

## Aggregate decision logic

```
any blocker      -> publish_decision = "blocked"
any warning      -> publish_decision = "blocked_with_warnings"
                    (still safe enough to publish but attention required)
otherwise        -> publish_decision = "publish_ready"
```

## Artifact digests captured per run

The orchestrator computes SHA-256 + size for every input artifact, so a release_packet has full provenance:

- `deck_contract_yaml`
- `workbook_contract_yaml`
- `deck_contract_schema_json`
- `workbook_contract_schema_json`
- `template_pptx`
- `visual_regions_yaml`
- `visual_baseline_json`
- `live_workbook_xlsx`
- `produced_pptx`

A future Track K (release catalog + waivers) can use these digests to bind waivers to specific artifact hashes — a waiver authored against a particular template SHA stays valid only while that exact template is in use.

## What landed

`scripts/build_release_packet.py` (new):

- `build_release_packet(workbook, pptx, ...)` runs all 8 validators in sequence and returns a single dict.
- CLI entry: `python3 scripts/build_release_packet.py --workbook PATH --pptx PATH --out PATH --md-out PATH`.
- `--skip-visual` flag for environments without `soffice`.
- Exit 0 on `publish_ready`, 1 otherwise.

`tests/test_track_g_release_packet.py` (new) — 4 tests:

- positive control: live anchors → publish_ready, 8/8 pass, 0 blockers, 0 warnings
- negative control: mutated contract (slide_count claim 15 vs actual 16) → publish_decision = blocked
- skip-visual: visual regression marked skipped, others pass → still publish_ready
- artifact_digests sanity: template_pptx digest is a valid 64-char SHA-256

`.github/workflows/track-e-validators.yml`:

- Path filter extended to `scripts/build_release_packet.py` and `tests/test_track_g_*.py`.
- Pytest now also runs `tests/test_track_g_*.py`.

## Tests

49 Track E + Track F + Track G tests pass (32 E + 13 F + 4 G). 703/703 unrelated tests pass.

## Hard NO-GOs preserved

- No edits to `scripts/build_deck_from_excel.py` in Track G-Lite (read-only orchestrator).
- No dashboard builder edits.
- No release catalog/waivers (Track K — which would build on top of this), OpenLineage (Track J), reusable workflows (Track L).
- `profiles.control_deck` stays deferred.

## What stays open

- **Track G full**: upstream ETL run, warehouse build, source-quality baselines, distribution comparison, semantic narrative report, SharePoint upload plan + evidence. All depend on data inputs not available in isolated sessions.
- **Track K**: release catalog + waivers binding to artifact_digests captured here.
- **Track J**: OpenLineage events for the orchestration steps.
- **Track L**: reusable workflows refactor.
- **Track E/M3 implementation**: conditional_inclusion field for the 2 conditional slides.

## Files

| File                | Purpose                                       |
| ------------------- | --------------------------------------------- |
| RESULTS.md          | this summary                                  |
| release_packet.json | post-orchestration release packet (sanitized) |
| release_packet.md   | post-orchestration release packet (markdown)  |
