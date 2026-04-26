# GPT Pro v3 — Contract-Driven Analytics Product Factory (Architecture Vision)

Date: 2026-04-25
Source: ChatGPT/GPT Pro (verbatim, captured after v2 review response)
Status: directional north star, not a single-cycle plan
Builds on: [`docs/2026-04-25-gpt-pro-review-response.md`](./2026-04-25-gpt-pro-review-response.md), [`docs/2026-04-25-gpt-pro-feedback-implementation-plan.md`](./2026-04-25-gpt-pro-feedback-implementation-plan.md)

---

## North Star

> Every source, transformation, metric, slide, table, chart, artifact, upload, and waiver should be declared in a contract, validated by code, linked by lineage, and reproducible from a snapshot date.

The v2 plan already points in that direction: Track B separates source quality actions, Tracks C/D add baselines and distribution checks, Track E externalizes `deck_contract.yaml`, and Track F makes the builder template-first with brand token enforcement. Those are the right foundations. The repo plan also correctly says "keep the source-backed lane, harden it, do not redesign," because the main hidden risk is still green runs on the wrong slice, not lack of a new platform.

---

## 1. Add a real "contract compiler"

Right now, the contract idea is spread across `source_contracts`, source requirements, validators, semantic checks, think-cell generation, and deck builder logic. The polish move is to create a compiler layer:

```
contracts/
  source_contracts/*.yaml
  metric_contract.yaml
  deck_contract.yaml
  artifact_contract.yaml
  release_contract.yaml
scripts/monthly_platform/contracts/
  compile_contracts.py
  source_contract.py
  metric_contract.py
  deck_contract.py
  artifact_contract.py
```

The compiler should emit a single runtime bundle:

```
output/contract_runtime/{snapshot_date}/{run_id}/
  compiled_source_contract.json
  compiled_metric_contract.json
  compiled_deck_contract.json
  compiled_artifact_contract.json
  compiled_release_contract.json
  contract_digest.json
```

That lets every stage consume the same normalized contract instead of each script interpreting YAML slightly differently. It also gives you one place to test schema versions, required fields, deprecated fields, default expansion, and compatibility.

**This closes a subtle class of defects: "builder and validator both pass, but they were reading different assumptions."**

## 2. Introduce canonical Parquet tables between ETL and deck

The deck should not build directly from extraction outputs or ad hoc JSON. Add a stable warehouse-like layer:

```
output/source_backed_warehouse/{snapshot_date}/{run_id}/
  raw/
    salesforce_report_extracts.parquet
    salesforce_list_view_extracts.parquet
  staged/
    opportunities.parquet
    accounts.parquet
    territories.parquet
  marts/
    director_book.parquet
    pipeline_coverage.parquet
    source_quality_findings.parquet
    release_metrics.parquet
```

DuckDB is a good fit here because it can read/write Parquet efficiently and query Parquet directly with SQL. DuckDB's docs describe Parquet as compressed columnar files and support direct `read_parquet` / `parquet_scan` access.

This would make the pipeline more plug-and-play because the deck builder, QA tests, think-cell exporter, release summary, and any future dashboard can all read the same marts. It also makes historical comparison much easier.

## 3. Add schema validation at two levels: dataframe and portable schema

Use two different schema concepts:

- **Pandera** for Python dataframe checks near extraction and transformation. Pandera can define dataframe schemas once and validate pandas, polars, dask, pyspark, and other dataframe types; it also supports column checks, parsing, and property-based testing.
- **Frictionless Table Schema** or JSON Schema for portable artifact contracts. Frictionless Table Schema is a language-agnostic JSON-expressible schema format for tabular data, including fields, types, constraints, primary keys, and foreign keys.

The combination is strong:

- Pandera = runtime validation inside Python code
- Table Schema / JSON Schema = portable contract for artifacts and downstream consumers

For example, `director_book.parquet` should have both:

```
schemas/pandera/director_book.py
schemas/table_schema/director_book.schema.json
```

That lets future tools consume the deck marts without reverse-engineering Python.

## 4. Make source-quality gates prove business coverage, not just extraction success

Track B/C/D already point this way. I would extend them into a formal "coverage audit":

```yaml
coverage_audit:
  required_business_segments:
    - stage_name
    - close_quarter
    - director
    - territory
    - owner
    - forecast_category
  checks:
    - no_missing_required_segment
    - no_unapproved_stage_drop
    - quarter_mix_vs_baseline
    - territory_mix_vs_baseline
    - owner_concentration_vs_baseline
    - open_closed_segmentation_vs_baseline
```

The implementation plan already calls for separate `zero_row_action`, `min_rows_action`, `max_rows_action`, `null_threshold_action`, `distribution_action`, and `expected_empty_conditions`, then baselines for row count, stage mix, and null rates. That is exactly the right direction.

The extra polish is to add **negative-control fixtures**:

```
tests/fixtures/bad_extracts/
  stage_5_missing/
  stale_source/
  report_hit_salesforce_cap/
  territory_dropped/
  null_rate_spike/
  duplicate_opportunities/
  wrong_quarter_mapping/
```

Each fixture should prove the gate fails. That turns "we think the gate works" into "we have a regression test showing it catches the failure mode."

## 5. Add metric contracts before a full semantic-layer tool

A full dbt/MetricFlow semantic layer may be premature, but the concept is useful. Add a lightweight `metric_contract.yaml` first:

```yaml
metrics:
  - id: open_pipeline_arr
    label: Open Pipeline ARR
    grain: opportunity
    expression: sum(amount_eur)
    filters:
      - is_open = true
    valid_period_roles:
      - current_quarter
      - next_quarter
    allowed_dimensions:
      - director
      - territory
      - stage_name
      - close_quarter
    source_mart: marts/opportunities.parquet
    owner: sales_ops
    deck_bindings:
      - slide_id: director_book
        table_id: tbl.director_book
        column_id: open_arr
```

Later, if the model layer becomes SQL-heavy, dbt contracts and dbt data tests become more attractive. dbt model contracts enforce that a model's returned dataset matches YAML-defined column names and data types, while dbt data tests can assert logic such as not-null, uniqueness, accepted values, relationships, and custom business rules.

For metrics specifically, MetricFlow powers the dbt Semantic Layer and defines metrics through YAML abstractions and SQL query generation. That is worth considering only when the repo has enough reusable metrics that hand-coded Python/JSON bindings become painful.

## 6. Make the deck contract the deck API

Track E is probably the highest-leverage build-side move. The plan already says `deck_contract.yaml` should define `schema_version`, `brand`, `data_bindings`, `slides[]`, required text, tables, render expectations, and think-cell range names.

I would push it further and make the deck contract the only accepted API between data and PowerPoint:

```yaml
slides:
  - id: pipeline_risk
    title: Pipeline Risk
    layout: simcorp_content
    data_dependencies:
      - mart: director_book
      - mart: source_quality_findings
    narrative:
      takeaway_metric: open_pipeline_arr
      required_takeaway: true
      required_action_owner: true
      max_takeaway_chars: 140
    tables:
      - id: tbl.pipeline_risk
        source_mart: director_book
        columns:
          - metric_id: director
          - metric_id: open_pipeline_arr
          - metric_id: stage_5_arr
          - metric_id: risk_rows
    charts:
      - id: cht.stage_mix
        source_mart: pipeline_stage_mix
        chart_type: bar
        required_named_range: StageMixChartData
    source_notes:
      required: true
      format: "Source: Salesforce {source_keys}; snapshot {snapshot_date}; run {run_id}"
```

Then every deck validator asks: "Does the PPTX satisfy the contract?" not "Does it look sort of like the old deck?"

The implementation plan's release blockers already point at this: artifact completeness, contract compliance, data binding, brand inheritance, layout integrity, render integrity, and executive readability.

## 7. Add template fingerprinting and visual golden tests

Template-first builder is necessary, but not enough. Add two gates:

```yaml
brand_template_gate: expected_template_sha256
  expected_theme_colors
  expected_fonts
  expected_layout_names
  expected_master_count
visual_regression_gate: render pptx -> slide images
  compare against approved baseline
  fail on high pixel/SSIM drift outside approved zones
```

This is how you turn "it rendered" into "it rendered like the SimCorp deck." The plan already recognizes render as necessary but insufficient and says brand inheritance plus golden-render regression should close that gap.

A good pattern is to allow dynamic regions and freeze static regions:

```yaml
visual_regression:
  slide_id: director_book
  frozen_regions:
    - title_area
    - footer_area
    - logo_area
    - source_note_area
  dynamic_regions:
    - main_table_area
```

That avoids false positives from changing numbers while still catching broken logos, wrong fonts, shifted footers, missing source notes, or layout drift.

## 8. Add lineage events for every stage

Today the evidence bundle is strong, but a lineage layer would make it much easier to answer: "Which Salesforce report fed this slide cell?"

OpenLineage is a good lightweight standard here. It defines a generic model of jobs, runs, and datasets, and supports extensible metadata facets.

A useful lineage event could look like:

```json
{
  "run_id": "live-all-sources-pipeline-open-v20d",
  "job": "build_source_backed_deck",
  "inputs": [
    "marts/director_book.parquet",
    "compiled_deck_contract.json",
    "SimCorp_PPT_Template.pptx"
  ],
  "outputs": ["source_backed_deck.pptx", "deck_render_manifest.json"],
  "facets": {
    "snapshot_date": "2026-04-30",
    "slide_ids": ["director_book", "pipeline_risk"],
    "contract_digest": "..."
  }
}
```

You do not need a full metadata platform immediately. Start by emitting OpenLineage-compatible JSON locally, then optionally wire Marquez or another backend later. The OpenLineage getting-started docs show collecting dataset/job/run metadata and visualizing dependencies through Marquez.

## 9. Add a release manifest that is human-readable and machine-readable

You already have release packets. The polish move is to standardize them as a signed release catalog:

```
release_summary.md
release_summary.json
release_decision.json
waivers.json
artifact_index.json
lineage_index.json
quality_findings.json
deck_contract_results.json
```

`release_summary.md` should answer:

- What changed?
- What data was used?
- What passed?
- What failed?
- What was waived?
- Who approved the waiver?
- When does the waiver expire?
- Which deck/workbook/upload artifacts were produced?
- Which source reports/list views fed each slide?

`release_summary.json` should make the same thing machine-readable.

This is especially useful when a future VP asks, "Why did the April deck say pipeline was down?" You can trace from slide → metric → mart → bundle → Salesforce extract → source key → owner → snapshot date.

## 10. Add a waiver system with expiration

Waivers are inevitable. Make them explicit:

```yaml
waivers:
  - id: WV-2026-04-001
    gate: source_quality.distribution.stage_mix
    source_key: pipeline_q2_open
    severity: warn
    reason: "Stage 5 mix changed after approved forecast cleanup."
    owner: "Sales Ops"
    approved_by: "Andre"
    expires_on: "2026-05-31"
    allowed_runs:
      - live-all-sources-pipeline-open-v20d
```

Rules:

- No anonymous waivers.
- No permanent waivers.
- No waiver without owner.
- No waiver without expiry.
- No waiver for hard truth/tie-out blockers unless explicitly allowed by release policy.

This prevents "temporary exception" from becoming hidden policy.

## 11. Modularize the pipeline into plug-in interfaces

The repo can become much more plug-and-play without changing platforms. Define interfaces like this:

```
SourceAdapter
  extract(contract, snapshot_date, run_id) -> ExtractResult
QualityPlugin
  validate(dataset, contract, baseline) -> QualityFindings
TransformPlugin
  build(inputs, contract) -> MartResult
MetricProvider
  resolve(metric_id, dimensions, filters, period) -> MetricFrame
DeckRenderer
  render(deck_contract, metric_provider, template) -> DeckArtifact
ArtifactPublisher
  publish(artifact, destination_contract) -> PublishResult
```

Then implementation becomes swappable:

```
SourceAdapter:
  SalesforceReportAdapter
  SalesforceListViewAdapter
  SharePointExcelAdapter
  LocalFixtureAdapter
QualityPlugin:
  RowCountQualityPlugin
  NullRateQualityPlugin
  DistributionQualityPlugin
  FreshnessQualityPlugin
DeckRenderer:
  PythonPptxRenderer
  ThinkCellWorkbookRenderer
  PdfRenderer
ArtifactPublisher:
  LocalPublisher
  GitHubArtifactPublisher
  SharePointPublisher
```

That gets you plug-and-play modularity without adding Airflow/Dagster immediately.

## 12. Use GitHub reusable workflows and composite actions

Track A cut over the workflow, but GitHub Actions can be made more modular too. Reusable workflows use `workflow_call` and can accept inputs/secrets from caller workflows, while composite actions bundle repeated steps into a single action step.

A good split:

```
.github/workflows/monthly-review.yml
  calls reusable-source-backed-pipeline.yml
.github/workflows/reusable-source-backed-pipeline.yml
  validates inputs
  sets up Python
  runs contracts
  runs pipeline
  uploads artifacts
.github/actions/setup-crm-analytics/action.yml
  install deps
  cache env
  validate repo paths
.github/actions/run-release-gates/action.yml
  run pytest
  run contract validators
  run deck validators
```

This makes scheduled, manual, PR, and release workflows call the same core logic.

## 13. Add an "Evidence/Quarto preview" as a companion, not a replacement

A web preview can make QA much easier before opening the deck. Evidence is an open-source framework for code-driven data products using SQL, including reports and decision-support tools.

A useful companion app could render:

```
/pages/monthly-review/
  Source Quality
  Pipeline Metrics
  Deck Bindings
  Slide-by-Slide QA
  Release Decision
```

This would not replace PowerPoint. It would be a QA cockpit that reads the same Parquet marts and contract outputs. That helps reviewers see source anomalies, baseline drift, and deck bindings before touching the PPTX.

## 14. Add AI-assisted review through a controlled artifact interface

This is optional, but powerful: expose artifacts to AI reviewers through a narrow, read-only interface. MCP is now a common standard for connecting AI applications to external systems, data sources, tools, and workflows.

For this repo, an internal MCP server could expose only:

```yaml
resources: latest_release_summary
  compiled_contracts
  quality_findings
  deck_contract_results
  lineage_index
  rendered_slide_images
tools: compare_runs(v20c, v20d)
  explain_metric_binding(slide_id, metric_id)
  list_unwaived_findings(run_id)
  trace_slide_to_source(slide_id)
```

Do not let an AI tool run Salesforce extraction, upload to SharePoint, or mutate release artifacts without explicit human approval. If you use MCP in a team setting, use an internal registry/allowlist approach; GitHub's Copilot docs describe MCP registries as HTTPS endpoints listing approved MCP servers and mention allowlist enforcement for local servers.

## 15. The tools to consider adding, in priority order

| Priority | Tool / pattern                                | Why it helps                                      | Add now?               |
| -------- | --------------------------------------------- | ------------------------------------------------- | ---------------------- |
| 1        | DuckDB + Parquet marts                        | Stable local warehouse layer between ETL and deck | Yes                    |
| 2        | Pandera                                       | Runtime dataframe schemas for Python transforms   | Yes                    |
| 3        | JSON Schema / Frictionless Table Schema       | Portable artifact and table contracts             | Yes                    |
| 4        | Contract compiler                             | One normalized contract bundle for all stages     | Yes                    |
| 5        | OpenLineage JSON events                       | Slide-to-source traceability                      | Yes, lightweight       |
| 6        | GitHub reusable workflows / composite actions | Modular CI/CD without new orchestrator            | Yes                    |
| 7        | Great Expectations                            | Richer production validation/checkpoints          | Maybe, after Track C/D |
| 8        | dbt + dbt tests/contracts                     | Best if transformations become SQL-model-heavy    | Maybe later            |
| 9        | MetricFlow / semantic layer                   | Central governed metrics                          | Later                  |
| 10       | Evidence.dev / Quarto preview                 | QA cockpit and companion report                   | Later                  |
| 11       | Dagster                                       | Asset orchestration, lineage, observability       | Later, not now         |
| 12       | MCP server                                    | AI-assisted review of artifacts/contracts         | Optional, guarded      |

Dagster is attractive long-term because it is built around data orchestration, integrated lineage, observability, declarative programming, and testability; its asset model defines assets as objects in persistent storage with compute functions and upstream dependencies. **But I would not switch to it now.** The repo plan explicitly says switching the runner to Airflow/Prefect/Dagster is out of scope for this cycle.

---

## Recommended Next "Polish Stack" — Sequence

After Track A is hardened and merged to main, the recommended sequence is:

1. Track B: policy action separation. ✅ **DONE** (commit `5303a5f`)
2. Track C/D: baselines + distribution checks.
3. Add DuckDB/Parquet mart layer.
4. Add Pandera schemas for raw/staged/mart tables.
5. Track E: `deck_contract.yaml`.
6. Track F: template-first builder.
7. Add `lineage_index.json` and `slide_to_source_map.json`.
8. Add golden visual regression.
9. Add `release_summary.md`/`json` with waivers.
10. Add reusable workflow / composite action cleanup.

That order matters. **It first makes the data trustworthy, then makes the deck deterministic, then makes the release explainable.**

---

## End-State Architecture

The strongest version of the system looks like this:

```
Salesforce sources
  ↓
source_contract.yaml + source owner/freshness/promotion metadata
  ↓
extract adapters
  ↓
raw Parquet + extraction manifest
  ↓
Pandera / quality plugins / source baselines / distribution audit
  ↓
staged Parquet
  ↓
metric_contract.yaml
  ↓
mart Parquet
  ↓
deck_contract.yaml
  ↓
template-first PPTX builder + think-cell workbook generator
  ↓
deck validators: contract, binding, brand, visual, render, semantics
  ↓
release packet: summary, lineage, waivers, artifacts, upload evidence
  ↓
SharePoint / GitHub artifact / reviewer cockpit
```

That is what "strong ETL to deck" looks like: not just a green run, but a **reproducible, contract-driven chain where a specific number on a slide can be traced all the way back to source selection, extraction, validation, transformation, metric definition, and release approval.**
