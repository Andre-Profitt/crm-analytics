# Track J-Lite — OpenLineage events + slide-to-source map (RESULTS)

## TL;DR

`scripts/monthly_platform/lineage.py` emits OpenLineage 2-0-2 events for every validator stage in the release-packet orchestrator. Two derived artifacts (`lineage_index.json`, `slide_to_source_map.json`) materialise alongside the events. Both Track G-Lite and Track K accept a `--lineage-dir` flag that turns the layer on. 76/76 release-pipeline tests green (66 prior + 10 new). Live APAC anchor smoke through `build_release_catalog.py` is `publish_ready` with 16 events, 8 jobs, 5 datasets, 16 slides mapped, 7 distinct workbook-sheet sources.

## Scope (Track J-Lite vs full Track J)

The full Track J spec (`docs/2026-04-25-gpt-pro-feedback-implementation-plan.md` §J) wraps `run_source_backed_monthly_pipeline.py` and `build_source_backed_deck.py`. Both depend on real ETL inputs — explicitly flagged in the prior session handoff as not isolated-session friendly. **Track J-Lite** scopes the lineage layer to what the release pipeline actually runs in CI today: the 8 validators in Track G-Lite + Track K. ETL stage emission is deferred until the Track G data lane is wired.

## Acceptance criteria

| #   | Criterion                                                                                                            | Result                                                                                 |
| --- | -------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| 1   | OpenLineage event emitter that produces START / COMPLETE / FAIL / ABORT envelopes per the 2-0-2 spec                 | ✅ `scripts/monthly_platform/lineage.py::LineageEmitter`                               |
| 2   | Per-event JSON files written under `<dir>/lineage_events/<seq>-<eventType>-<job>.json`                               | ✅ filename convention deterministic; 16 files emitted on the live anchor              |
| 3   | Aggregated DAG-view `lineage_index.json` with jobs/datasets/edges + summary                                          | ✅ `lineage.build_lineage_index`                                                       |
| 4   | `slide_to_source_map.json` answering "which sources fed each slide?"                                                 | ✅ `lineage.build_slide_to_source_map` — 16 slides, 7 distinct workbook-sheet datasets |
| 5   | `build_release_packet.py` accepts `--lineage-dir` and emits all three artifacts                                      | ✅                                                                                     |
| 6   | `build_release_catalog.py` (Track K wrapper) accepts `--lineage-dir`, surfaces a `lineage` block in the catalog JSON | ✅                                                                                     |
| 7   | Tests: emitter shape, index aggregation, slide-map coverage, end-to-end orchestrator integration                     | ✅ 10/10 in `tests/test_track_j_lineage.py`                                            |
| 8   | No regression in 66 pre-existing track-{e,f,g,k,em2} tests                                                           | ✅ 76/76 pass (66 prior + 10 new)                                                      |
| 9   | CI workflow extended to gate the new module + tests                                                                  | ✅ `.github/workflows/track-e-validators.yml`                                          |
| 10  | Live APAC anchor smoke through `build_release_catalog.py` stays `publish_ready` with lineage on                      | ✅                                                                                     |

## Live smoke (APAC anchor, with `--lineage-dir`)

```bash
python3 scripts/build_release_catalog.py \
  --workbook ~/Downloads/jesper-tyrer-2026-04-20.xlsx \
  --pptx ~/Downloads/jesper-tyrer-LAND.pptx \
  --skip-visual \
  --run-id track-j-smoke \
  --lineage-dir output/track_j/catalog-smoke \
  --out output/track_j/release_catalog.json
```

Output:

```
catalog: output/track_j/release_catalog.json
release_catalog: publish_ready (pre: 0 blockers / 0 warnings; post: 0 blockers / 0 warnings; applied=0 unused=1)
```

Lineage block in the catalog JSON:

```json
{
  "run_id": "3b4c9893-af19-4d86-ab2d-e4d8df439a03",
  "events_dir": "output/track_j/catalog-smoke/lineage_events",
  "lineage_index_path": "output/track_j/catalog-smoke/lineage_index.json",
  "slide_to_source_map_path": "output/track_j/catalog-smoke/slide_to_source_map.json",
  "event_count": 16,
  "job_count": 8,
  "dataset_count": 5,
  "slide_count": 16,
  "distinct_dataset_count": 7
}
```

## Event shape (sample)

A `COMPLETE` event from the live smoke (excerpt; full file in `event_sample-COMPLETE-deck_contract.json`):

```json
{
  "eventType": "COMPLETE",
  "eventTime": "2026-04-27T22:55:07.275357+00:00",
  "producer": "https://github.com/anthropic-internal/crm-analytics/track-j-lite",
  "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
  "run": {
    "runId": "<uuid>",
    "facets": {
      "timing": { "start_time": "...", "end_time": "..." },
      "result": { "validator_status": "pass", "blockers": 0, "warnings": 0 }
    }
  },
  "job": {
    "namespace": "crm-analytics.monthly-release",
    "name": "deck_contract"
  }
}
```

START events carry the inputs[]; COMPLETE events carry result facets (`validator_status`, blocker / warning counts).

## Lineage index summary (live anchor)

```
job_count: 8
dataset_count: 5
edge_count: 14
jobs_complete: 7
jobs_failed: 1     # ABORT — visual_regression skipped via --skip-visual
jobs_open: 0
```

Datasets identified by digest:

- `config/deck_contract.yaml`
- `config/director_workbook_contract.yaml`
- `assets/SimCorp_PPT_Template.pptx`
- `~/Downloads/jesper-tyrer-2026-04-20.xlsx`
- `~/Downloads/jesper-tyrer-LAND.pptx`

## Slide-to-source map (live anchor)

- 16 active slides (matches Track E's 16-slide active baseline; conditional slides remain unmapped until the cadence env wires them).
- 7 distinct workbook-sheet datasets fed at least one table:
  - `director_workbook::Commercial Approval`
  - `director_workbook::Commit Items`
  - `director_workbook::Pipeline Inspection`
  - `director_workbook::Pipeline Open FY26`
  - `director_workbook::Q1 Movement`
  - `director_workbook::Renewals FY26`
  - `director_workbook::Won Lost FY26`

Per-slide entries include: kind (table/takeaway/source_note/link/static), binding_type, source/sheet, physical_columns, snapshot_roles, transform_id, and resolution_status (cross-referenced from the binding resolver report).

## Test results

```
$ python3 -m pytest tests/test_track_e_*.py tests/test_track_f_*.py \
    tests/test_track_g_*.py tests/test_track_em2_*.py \
    tests/test_track_k_*.py tests/test_track_j_*.py
76 passed in 21.72s
```

Track J-Lite tests (10):

- `test_emitter_writes_start_complete_pair` — envelope conformance + filename convention
- `test_emitter_status_must_be_terminal` — only COMPLETE/FAIL/ABORT accepted as terminal status
- `test_emitter_handles_fail_and_abort` — non-pass terminal events serialise correctly
- `test_file_dataset_includes_content_digest` — sha256 + size facets on present files; lifecycleState on missing
- `test_lineage_index_aggregates_jobs_and_datasets` — index aggregation sanity
- `test_lineage_index_marks_failed_and_open_jobs` — open / failed job classification
- `test_slide_map_covers_every_active_slide_in_director_monthly` — 16 active slides each carry ≥1 source binding
- `test_slide_map_distinct_datasets_aggregate_workbook_sheets` — distinct-dataset aggregation
- `test_slide_map_resolution_status_uses_binding_report` — slide-map status comes from resolver, not a default (live anchor)
- `test_release_packet_emits_lineage_artifacts` — orchestrator end-to-end (live anchor)

## Files touched

New:

- `scripts/monthly_platform/lineage.py` (~330 lines)
- `tests/test_track_j_lineage.py` (~230 lines)
- `docs/review-artifacts/track-j/RESULTS.md` (this file)
- `docs/review-artifacts/track-j/lineage_index.sample.json`
- `docs/review-artifacts/track-j/slide_to_source_map.sample.json`
- `docs/review-artifacts/track-j/event_sample-COMPLETE-deck_contract.json`

Modified:

- `scripts/build_release_packet.py` — wires lineage emission per validator stage; adds `--lineage-dir`
- `scripts/build_release_catalog.py` — passes `--lineage-dir` through; surfaces `lineage` in catalog JSON
- `scripts/monthly_platform/release_catalog.py` — accepts `lineage_dir` kwarg; surfaces lineage in `as_dict()`
- `.github/workflows/track-e-validators.yml` — adds new module + test pattern to gates

## Hard rules preserved

- Builder lineage scope unchanged — no new `build_*.py` files touched. Pure metadata layer.
- Never-waivable gates list (`config/release_policy.yaml`) unchanged.
- 16-slide active contract unchanged. Conditional slides remain in `conditional_slides[]`.
- CLI-first; no MCP tools.
- `profiles.control_deck` still `status: deferred`.

## Forward state

- **Full Track J** still pending: instrumenting `run_source_backed_monthly_pipeline.py` and `build_source_backed_deck.py` requires the Track G data lane to be wired. Track J-Lite is the orchestrator-side half; the ETL-side half can adopt the same `LineageEmitter` API verbatim.
- **Marquez backend** (or any OpenLineage consumer) can be wired to point at `output/.../lineage_events/` whenever the team wants live observability — no code changes required, just an out-of-band collector.
- **slide_to_source_map** currently identifies workbook sheets as datasets. Once the source-extractor wiring (M2 cleanup ticket) flips datasets from `optional_empty` to `source_backed`, the map should also enumerate the upstream Salesforce list-view IDs.
