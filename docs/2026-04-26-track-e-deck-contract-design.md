# Track E — Deck contract foundation (Milestone 1 design)

- **Integration branch:** `integration/track-e-deck-contract`
- **Anchored ETL baseline:** `docs/checkpoints/v20d-etl-spine-freeze-2026-04-30/` (Milestone 0, ETL 1.0)
- **Status:** scoping → implementation
- **Hard NO-GO this milestone:** PowerPoint builder edits, template-first refactor, OpenLineage events, waivers, reusable workflows. Track F/G/J/K/L are downstream.

## Why this milestone exists

After Track I closed, every warehouse table on the source-backed lane has both a Pandera schema and a Frictionless Table Schema descriptor; the v20d freeze proved 7/7 tables validate, parity 8/8, contract suite 96/96. The ETL spine is now trustworthy.

The next failure mode is _not_ "Stage 5 disappeared." It is **"the deck consumes good data in an ungoverned way"** — the deck binds the wrong column, labels the wrong period, drops a source note, or visually drifts from the SimCorp master while the underlying mart is fine.

Track E closes that boundary. It defines a **single declarative contract** for what the deck is allowed to show, then proves every existing slide/table/metric maps to a validated mart column. **No PowerPoint generation changes ship in this milestone.** Track F (template-first builder) consumes the contract; Track G runs a full v20d release pass against it.

## Scope

### In scope

1. `config/deck_contract.yaml` — declarative contract: brand template fingerprint, allowed marts (with schema_id back-references), per-slide bindings (id, slide_number, title, layout, required takeaway, table/metric bindings, required source notes).
2. `schemas/deck_contract.schema.json` — strict JSON Schema (draft 2020-12) describing the YAML shape.
3. `scripts/monthly_platform/deck_contract.py` — loader + structural validator. Returns a typed object; raises on malformed/unknown references.
4. `scripts/validate_deck_contract.py` — CLI entry point: validates YAML against JSON Schema, then runs cross-reference checks (every `mart` referenced has a registered schema; every `column` referenced exists on that mart's Pandera schema).
5. `scripts/monthly_platform/deck_binding_resolver.py` — resolves contract bindings against an actual warehouse run directory (`output/source_backed_warehouse/<snapshot>/<run_id>/`). Verifies that every bound mart parquet exists, every bound column is present in the observed_schemas, and the manifest is from a contract-validated run.
6. `scripts/validate_deck_bindings.py` — CLI entry point: emits `deck_binding_report.json` + `.md` next to the warehouse run.
7. `tests/test_deck_contract.py` — happy path + negative controls (see fixture list below).
8. `tests/test_deck_binding_resolver.py` — happy path against the v20d freeze fixtures + negative controls (missing parquet, unknown column, schema mismatch).
9. `tests/fixtures/deck_contract/` — minimal good contract + each negative-control variant.

### Out of scope (deferred to Track F+)

- Editing `scripts/build_source_backed_deck.py`. The builder still hard-codes brand colors and slide titles; that's Track F.
- Loading the SimCorp template. The contract _names_ it and asserts a SHA-256, but Milestone 1 only checks the file exists and computes the digest — it does not switch the builder to `Presentation(template)`.
- Visual regression (golden images). Track F.
- Render/overflow gates. Track F.
- Release catalog, waivers, lineage events. Tracks K/J.

## What the contract describes

The current `build_source_backed_deck.py` produces a 6-slide deck with these titles (verbatim from `SLIDE_TITLES`):

1. Monthly Sales Director Operating Review
2. Publish gate status is green across the evidence chain
3. Regional open pipeline is traceable to director bundles
4. Director book view: pipeline, risk, and tie-out health
5. Quarter display logic: current quarter unless fallback is required
6. Leadership readout: clean source chain, ready for standard deck production

These slides currently consume a `truth_packet` JSON blob (legacy aggregate). The contract instead binds slides to **validated warehouse marts** so every displayed metric has a contract-validated lineage:

| Mart / staged table              | Schema id                        | What it provides                                                                |
| -------------------------------- | -------------------------------- | ------------------------------------------------------------------------------- |
| `mart_source_run_summary`        | `mart_source_run_summary`        | One row per run: status, source/finding counts, baseline + distribution rollup. |
| `mart_director_source_health`    | `mart_director_source_health`    | Per-director source health: ok/warning/blocked, total rows, total findings.     |
| `staged_source_quality_findings` | `staged_source_quality_findings` | Track B/C findings.                                                             |
| `staged_distribution_findings`   | `staged_distribution_findings`   | Track D distribution findings.                                                  |
| `staged_source_requirements`     | `staged_source_requirements`     | Source contract requirements (per source).                                      |
| `raw_source_quality_audit`       | `raw_source_quality_audit`       | Per-source extract status + row counts.                                         |
| `raw_salesforce_extract_plan`    | `raw_salesforce_extract_plan`    | Configured/territory extract scope.                                             |

The contract is the **single source of truth** for which marts the deck reads, which columns it may display, and what each slide owes the reader (title, takeaway, source notes).

## Contract shape (overview)

```yaml
schema_version: monthly_platform.deck_contract.v1
brand:
  template: assets/SimCorp_PPT_Template.pptx
  expected_template_sha256: <computed during M1 implementation>
data_sources:
  marts:
    source_run_summary:
      path: marts/source_run_summary.parquet
      schema_id: mart_source_run_summary
    director_source_health:
      path: marts/director_source_health.parquet
      schema_id: mart_director_source_health
    # ... 5 more
slides:
  - id: cover
    slide_number: 1
    title: Monthly Sales Director Operating Review
    layout: simcorp_title_summary
    required_takeaway: true
    max_takeaway_chars: 200
    required_source_notes: ["snapshot_date", "run_id"]
    metrics:
      - id: director_count
        mart: director_source_health
        column: director
        aggregation: distinct_count
      # ...
  - id: publish_gate
    slide_number: 2
    # ...
```

The full skeleton lives in `config/deck_contract.yaml`. Every `mart`/`column` reference must resolve to a registered schema_id and a column declared on that schema — the validator enforces this.

## Acceptance criteria (Milestone 1 done means)

1. `config/deck_contract.yaml` exists, validates against `schemas/deck_contract.schema.json`, and binds all 6 existing slides to validated marts.
2. Every `metrics[].mart` and `tables[].mart` references a `data_sources.marts.<key>` entry.
3. Every `metrics[].column` and `tables[].columns[*]` is a real column on the referenced Pandera schema (cross-checked at validate time, not just in the YAML).
4. Every slide has: `id`, `slide_number`, `title`, `layout`, `required_takeaway` flag, at least one `required_source_note`, and at least one binding (metric or table).
5. Negative-control fixtures fail validation:
   - `missing_slide_id`, `duplicate_slide_number`, `unknown_mart`, `unknown_column`, `missing_required_takeaway`, `invalid_table_binding`, `invalid_schema_id`, `missing_source_note`, `invalid_template_path`.
6. `scripts/validate_deck_contract.py --contract config/deck_contract.yaml` exits 0 on the canonical contract.
7. `scripts/validate_deck_bindings.py --warehouse-run output/source_backed_warehouse/2026-04-30/v20d-etl-spine-freeze --contract config/deck_contract.yaml` exits 0 against the freeze evidence and emits `deck_binding_report.json` with `status: pass`, `unknown_column_count: 0`, `missing_binding_count: 0`.
8. New tests pass; existing 96/96 contract suite stays green.
9. No edits to `scripts/build_source_backed_deck.py` or any `build_*.py`.
10. Milestone release note (this doc) explains what risk Track E closes and what stays open for Track F.

## What risk this closes vs. leaves open

**Closes:**

- Deck-displayed metrics can no longer reference unvalidated mart columns; the validator rejects unknown bindings before any deck is built.
- Slide identity (id, slide_number, required text) is declared once and shared by future render/visual gates.
- The template path/digest is captured, so the builder can later assert "this deck was built from approved brand assets" without inventing a new contract.

**Stays open (Track F+):**

- The builder still constructs slides with hard-coded RGB constants and `Presentation()` (no master inheritance).
- No render-time gates: text/table overflow, missing footer, missing source note still go undetected at build time.
- No visual regression baseline.
- No release catalog or waiver system.
- No OpenLineage event for `validate_deck_contract` / `validate_deck_bindings`.

## Negative controls (fixture matrix)

`tests/fixtures/deck_contract/`:

| Fixture                          | What's wrong                                                               | Expected validator failure          |
| -------------------------------- | -------------------------------------------------------------------------- | ----------------------------------- |
| `good.yaml`                      | nothing                                                                    | passes                              |
| `missing_slide_id.yaml`          | first slide has no `id`                                                    | JSON Schema: required `id`          |
| `duplicate_slide_number.yaml`    | two slides share `slide_number: 2`                                         | cross-check: duplicate slide_number |
| `unknown_mart.yaml`              | metric refs `data_sources.marts.foo` that doesn't exist                    | cross-check: unknown mart key       |
| `unknown_column.yaml`            | metric refs valid mart but column not on Pandera schema                    | cross-check: unknown column         |
| `missing_required_takeaway.yaml` | slide has `required_takeaway: true` but no `takeaway` block                | cross-check                         |
| `invalid_table_binding.yaml`     | `tables[0]` has no `id` or no `columns`                                    | JSON Schema: required fields        |
| `invalid_schema_id.yaml`         | `data_sources.marts.x.schema_id` doesn't match a registered Pandera schema | cross-check                         |
| `missing_source_note.yaml`       | slide `required_source_notes` is empty list                                | JSON Schema: minItems 1             |
| `invalid_template_path.yaml`     | `brand.template` points to a missing file                                  | cross-check                         |

## Implementation order

1. Write this design doc (this file). ✅ on commit.
2. Compute template SHA-256, write `config/deck_contract.yaml` skeleton with all 6 slides bound.
3. Write `schemas/deck_contract.schema.json`.
4. Write loader + cross-reference validator (`scripts/monthly_platform/deck_contract.py`).
5. Write CLI (`scripts/validate_deck_contract.py`).
6. Write resolver + CLI (`deck_binding_resolver.py`, `validate_deck_bindings.py`).
7. Add tests + fixtures.
8. Run the full validation against the v20d freeze evidence; commit `deck_binding_report.json` next to the freeze.
9. Update memory: Milestone 1 status.

## Operating rules for this milestone

Per the release-train operating model:

- **One integration branch** (`integration/track-e-deck-contract`). Sub-PRs allowed underneath, all roll up to one milestone decision.
- **No micro-slice patching mid-milestone.** Reviews are at acceptance-criteria boundaries.
- **Every gate has a bad fixture.** A validator that has never failed on something bad is not a gate.
- **No new architecture in this milestone.** No semantic layer, no MCP server, no Dagster, no Evidence cockpit. Those are all premature until Track G ships.
