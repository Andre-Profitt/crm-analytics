# Track K — release catalog + waivers (results)

Branch: `integration/track-f-template-first-builder`
Predecessor: M3 implementation commit `1971bb9`

## What this is

Track K wraps Track G-Lite's release packet with a strict waiver system. Findings the validators emit get checked against waivers loaded from `config/waivers/`; matching waivers downgrade severity. Final `publish_decision` reflects post-waiver totals.

## Acceptance gate

| GPT criterion                                                                                                             | Status                       |
| ------------------------------------------------------------------------------------------------------------------------- | ---------------------------- |
| `config/release_policy.yaml` exists                                                                                       | **pass**                     |
| `config/waivers/*.yaml` waiver directory                                                                                  | **pass** (1 example fixture) |
| Waiver loader rejects: missing owner, missing expiry, anonymous, permanent, unbound, no-op severity, never-waivable gates | **pass**                     |
| Release catalog JSON + Markdown                                                                                           | **pass**                     |
| Waiver application downgrades severity, recomputes totals, captures applied + unused waivers                              | **pass**                     |
| Strict-rule tests cover the failure paths                                                                                 | **pass** (13 tests)          |

## Validator state

```
release_catalog: publish_ready
                 (pre: 0 blockers / 0 warnings;
                  post: 0 blockers / 0 warnings;
                  applied=0 unused=1)
```

The example waiver `WV-2026-04-001` (template_size_mismatch) is unused — no findings to match. That's expected; it's a documentation/fixture waiver.

## Strict waiver rules (per GPT's release-train spec)

Every waiver MUST:

- Match `^WV-\d{4}-\d{2}-\d{3}$` ID format
- Carry an `owner` (non-empty)
- Carry an `approved_by` (non-empty)
- Carry a `reason` (≥10 chars; "no one-liners")
- Specify `severity_before` ∈ `{blocker, warning}` and `severity_after` ∈ `{info, warning, waived}` such that `severity_after < severity_before` (no upgrades, no no-ops)
- Carry an ISO-date `expires_on` in the future
- NOT target a gate listed in `release_policy.never_waivable`

Violations are rejected at load time. The release catalog records the rejection as a `waiver_invalid` finding so auditors see what was attempted.

## Never-waivable gates

`config/release_policy.yaml::never_waivable` lists 9 finding codes that govern truth or brand identity and must always block release:

- `deck_contract.schema_violation`
- `director_workbook_contract.schema_violation`
- `brand_fingerprint.template_sha256_mismatch`
- `brand_fingerprint.required_layout_missing`
- `pptx_contract.table_header_mismatch`
- `pptx_contract.table_count_mismatch`
- `pptx_contract.title_neither_stable_nor_legacy`
- `director_workbook_validation.missing_sheet`
- `director_workbook_validation.missing_required_column`

Adding a new gate to this list locks future waivers out without code changes.

## Release catalog output

`output/track_k/release_catalog.json` carries:

- `publish_decision` (publish_ready | blocked_with_warnings | blocked)
- `pre_waiver_blocker_total` and `pre_waiver_warning_total`
- `post_waiver_blocker_total` and `post_waiver_warning_total`
- `applied_waivers[]` — each match: waiver_id, owner, approved_by, gate, severity before/after, finding message + path
- `unused_waivers[]` — declared but no matching findings
- `waiver_loader_findings[]` — invalid/expired/parse-error waivers
- `artifact_digests` — SHA-256 + size for every input artifact (inherited from Track G-Lite)
- `release_packet_summaries` — per-validator status from Track G-Lite

The catalog is the auditable artifact: a reviewer can answer "why was this published?" or "why was this blocked?" entirely from the JSON.

## What landed

`config/release_policy.yaml` (new):

- 9 never-waivable gates
- empty `severity_overrides` (no validator severity flips today)

`config/waivers/EXAMPLE-template-size-mismatch.yaml` (new):

- One demonstrative waiver (no-op against canonical contract)

`scripts/monthly_platform/waivers.py` (new):

- `Waiver` and `ReleasePolicy` dataclasses
- `load_policy()`, `load_waivers()`
- Strict `_parse_waiver()` enforcing all 9 GPT rules

`scripts/monthly_platform/release_catalog.py` (new):

- `build_release_catalog(workbook, pptx, ...)` — wraps Track G-Lite, applies waivers, recomputes decision
- `WaiverApplication` and `CatalogResult` dataclasses
- `render_markdown()` for human-readable summary

`scripts/build_release_catalog.py` (new):

- CLI: `python3 scripts/build_release_catalog.py --workbook PATH --pptx PATH --run-id ID --out PATH --md-out PATH`
- Exit 0 on `publish_ready`, 1 otherwise

`tests/test_track_k_release_catalog.py` (new) — 13 tests:

- Policy loader has the never-waivable list
- Canonical waiver loader passes (1 example fixture)
- Invalid ID format rejected
- Missing owner rejected
- Short reason rejected
- Severity no-op rejected
- Severity upgrade rejected
- Never-waivable gate rejected
- Expired waiver filtered (and surfaced as `waiver_expired`)
- `applies_to_run` filter works (run-specific + all-runs)
- Release catalog publish_ready with canonical inputs
- Release catalog applies a matching waiver and downgrades it
- `render_markdown` includes the decision

`.github/workflows/track-e-validators.yml`:

- Path filter extended to all Track K paths
- Pytest runs `tests/test_track_k_*.py` and `tests/test_track_em2_*.py`

## Tests

- 13 Track K tests pass
- 66 Track E + F + G + M2 + K tests total pass
- 716/716 unrelated tests pass

## Hard NO-GOs preserved

- No edits to `scripts/build_deck_from_excel.py` in Track K (read-only release tier).
- No dashboard builder edits.
- `profiles.control_deck` stays deferred.
- Track J (OpenLineage) and Track L (reusable workflows) remain separate.

## Files

| File                 | Purpose                                        |
| -------------------- | ---------------------------------------------- |
| RESULTS.md           | this summary                                   |
| release_catalog.json | post-orchestration release catalog (sanitized) |
| release_catalog.md   | post-orchestration release catalog (markdown)  |
