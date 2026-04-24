# Source-Backed Monthly Runner Plan

Scope: add a deterministic source-backed lane to the orchestrator runner without replacing the existing workbook/Office lane. The runner should own command sequencing and gates; Claude/Codex workers should own non-overlapping files only.

## Operative Assumption

Use `2026-04-30` / `live-all-sources-pipeline-open-v18` as the proven live baseline. The lane is publish-eligible only when every gate exits `0` and reports `status=ok` or `ready`, except the pre-extraction source-contract pass may report `warning` when bundles are not built yet and `high_finding_count=0`. The post-bundle final source-contract gate must be clean, and the business/source/display quarter mapping must be explicit and approved.

## Command Order

Run from `/Users/test/crm-analytics`. The period-aware launcher is the canonical operator command; it resolves the prior month-end snapshot when `--snapshot-date` is omitted and creates a timestamped run ID to avoid artifact collisions:

```bash
python3 scripts/run_source_backed_monthly_default.py \
  --snapshot-date 2026-04-30
```

For an exact replay of the proven live baseline, use:

```bash
RUN_DATE=2026-04-30
RUN_ID=live-all-sources-pipeline-open-v18

python3 scripts/run_source_backed_monthly_pipeline.py \
  --snapshot-date "$RUN_DATE" \
  --run-id "$RUN_ID" \
  --sharepoint-upload \
  --output-path "output/source_backed_monthly_pipeline_runs/$RUN_DATE/$RUN_ID/pipeline_run_manifest.json"
```

Expanded gates: YAML authoring sync, PI list-view audit, source contract preflight, source requirement lint, Salesforce extraction, source bundles, DirectorBundles, final source contract, dataset readiness, analyst workbook, think-cell source, publish gate, Gold analytics, deck truth packet, source-backed deck, post-build language polish, deck visual audit, table-format contract audit, semantic/business-readiness audit, headless render audit, release bundle / SharePoint handoff manifest, SharePoint upload dry-run plan, and optional live SharePoint upload stage.

## Inputs

- Salesforce org: `apro@simcorp.com` on `simcorp.my.salesforce.com`.
- Source requirements: `config/monthly_source_requirements.json`.
- Territory config and report/list-view IDs: `config/sd_monthly_territories.json`.
- DirectorBundle publish contract: `config/monthly_director_bundle_contract.json`.
- Salesforce field guardrails: `config/salesforce_field_guardrails.json`.
- YAML authoring contract: `config/source_contracts/sales_director_monthly.yaml`; compiled runtime targets are checked by `scripts/compile_monthly_source_contract_config.py` before Salesforce extraction.
- Quarter policy: centralized in `scripts/monthly_platform/period.py`; current source registry is explicitly `calendar_quarter`, while business period labels are separately mapped to the fiscal calendar and gated by quarter-mapping approval.
- Optional source audit fold-in: `output/source_contract_audit/2026-04-23/source_contract_audit.json`.

## Outputs

- YAML authoring check: `output/monthly_source_contract_authoring/2026-04-30/live-all-sources-pipeline-open-v18/source_contract_authoring_check.json`.
- Source contract: `output/monthly_source_contract/live-all-sources-pipeline-open-v18/2026-04-30/monthly_source_contract.json`.
- Source contract lint: `output/monthly_source_contract/2026-04-30/live-all-sources-pipeline-open-v18/source_contract_lint.json`.
- Raw/normalized Salesforce extracts: `output/monthly_salesforce_sources/2026-04-30/live-all-sources-pipeline-open-v18/run_manifest.json`.
- Territory source bundles: `output/monthly_source_bundles/2026-04-30/live-all-sources-pipeline-open-v18/source_bundle_manifest.json`.
- Source-backed DirectorBundles: `output/monthly_director_bundles_from_sources/2026-04-30/live-all-sources-pipeline-open-v18/director_bundle_manifest.json`.
- Analyst workbook: `output/monthly_director_bundles_from_sources/2026-04-30/live-all-sources-pipeline-open-v18/source_backed_analyst_workbook.xlsx`.
- think-cell source artifacts: `output/thinkcell_source_from_bundles/2026-04-30/live-all-sources-pipeline-open-v18/thinkcell_source.xlsx` and `output/thinkcell_source_from_bundles/2026-04-30/live-all-sources-pipeline-open-v18/thinkcell_data.ppttc`.
- Publish gate: `output/monthly_source_backed_publish_gate/2026-04-30/live-all-sources-pipeline-open-v18/source_backed_publish_gate.json`.
- Truth packet: `output/deck_truth_packets_from_sources/live-all-sources-pipeline-open-v18/2026-04-30/deck_truth_packet.json`.
- Review deck: `output/source_backed_decks/2026-04-30/live-all-sources-pipeline-open-v18/source_backed_monthly_review.pptx`.
- Polish audit: `output/source_backed_deck_polish/2026-04-30/live-all-sources-pipeline-open-v18/source_backed_deck_polish_audit.json`.
- Visual audit: `output/source_backed_deck_visuals/2026-04-30/live-all-sources-pipeline-open-v18/source_backed_deck_visual_audit.json`.
- Table contract audit: `output/source_backed_deck_table_contract/2026-04-30/live-all-sources-pipeline-open-v18/source_backed_deck_table_contract_audit.json`.
- Semantic audit: `output/source_backed_deck_semantics/2026-04-30/live-all-sources-pipeline-open-v18/source_backed_deck_semantic_audit.json`.
- Render audit: `output/source_backed_deck_renders/2026-04-30/live-all-sources-pipeline-open-v18/source_backed_deck_render_audit.json`.
- Runner manifest: `output/source_backed_monthly_pipeline_runs/2026-04-30/live-all-sources-pipeline-open-v18/pipeline_run_manifest.json`.
- Release packet: `output/monthly_review_release_packets/2026-04-30/live-all-sources-pipeline-open-v18/source_backed_release_packet.json`.
- Release bundle manifest: `output/source_backed_release_bundles/2026-04-30/live-all-sources-pipeline-open-v18/source_backed_release_bundle_manifest.json`.
- Release bundle zip: `output/source_backed_release_bundles/2026-04-30/live-all-sources-pipeline-open-v18/source_backed_release_bundle.zip`.
- SharePoint upload plan: `output/source_backed_sharepoint_upload_plans/2026-04-30/live-all-sources-pipeline-open-v18/sharepoint_upload_plan.json`.
- SharePoint upload result: `output/source_backed_sharepoint_uploads/2026-04-30/live-all-sources-pipeline-open-v18/sharepoint_upload_result.json`.
- Latest source-backed aliases: `output/source_backed_monthly_pipeline_runs/latest.json`, `output/source_backed_monthly_pipeline_runs/latest.md`, `output/source_backed_monthly_pipeline_runs/2026-04-30/latest.json`, and `output/source_backed_monthly_pipeline_runs/2026-04-30/latest.md`.

## Failure Gates

- Stop before extraction if YAML authoring sync reports drift/missing compiled JSON.
- Stop before extraction if the source contract is `blocked` or any required source ID is missing.
- Stop before extraction if source requirement lint finds a publish-required dataset without an enabled source requirement.
- Stop if live extraction exits non-zero, `run_manifest.status != ok`, any stage is not `ok`, or selected extracts are missing.
- Stop if source bundle build exits `2`, reports `status=blocked`, or `missing_selected_source_count > 0`.
- Stop if DirectorBundle build exits `2`, reports `status=blocked`, emits validation findings, or produces fewer than `9` bundles.
- Stop if the final source contract has any high/warning finding, missing report ID, or missing bundle.
- Stop if dataset readiness is not `ready`; current publish-required check is `pipeline_open`, with `pi_current`, `pi_forward`, and `snapshot_trend` verified through DirectorBundle coverage.
- Stop if list-view audit is not `ok` or `finding_count > 0`.
- Stop if `validate_monthly_source_backed_run.py` reports any finding; live v18 has `55` source extracts, `9` source bundles, `9` DirectorBundles, `4` publish-required datasets, and `0` findings.
- Stop if truth packet status is `blocked`, `high_blocker_count > 0`, or `tieout_mismatch_count > 0`.
- Stop if source-backed deck visual audit is not `ok`; live v18 has `6` slides and `0` visual findings.
- Stop if polish audit is not `ok`; live v18 applies `43` deterministic language replacements and has `0` findings.
- Stop if table contract audit is not `ok`; live v18 validates `5` standard tables, exact headers, navy/white headers, and approved body palette with `0` findings.
- Stop if semantic audit is not `ok`; live v18 has `0` semantic findings and human-style score `100`.
- Stop if headless render audit is not `ok`; live v18 renders `6` slide PNGs from PDF with `0` findings and writes a montage for human review.
- Stop if release bundle manifest is not `ok`, any required artifact is missing, the zip is missing, or the SharePoint handoff reports `upload_ready=false`.
- Stop if SharePoint upload dry-run is not `planned`, plans fewer than `1` asset, or reports any missing publish asset.
- When `--sharepoint-upload` is set, stop if the live SharePoint upload stage is not `ok`, uploads fewer than `1` asset, or skips any planned asset.
- Release packet blocks publish unless all `21` release checks pass: runner status, required stages, YAML authoring sync, source-contract preflight, source-contract lint, final source-contract cleanliness, explicit quarter-policy lock, quarter-mapping approval, PI list-view audit, Salesforce extract completeness, SourceBundle completeness, DirectorBundle completeness, publish gate cleanliness, truth packet cleanliness, deck visual completeness, polish cleanliness, table-format contract, semantic readiness, rendered PDF/PNG completeness, release bundle completeness, and SharePoint upload-plan completeness.

## Live 2026-04-30 Baseline

- Extract/source status: `ok`; `55` selected / `55` extracted / `0` missing.
- Runner status: `ok`; `23` gates passed; YAML authoring sync `ok` with `3` targets and `0` drift; source requirement lint `ok` with `0` findings; final source contract `ok` with `0` warnings; release packet says `publish`.
- Quarter policy lock: source registry/display use `Q2 2026`; business reporting maps to fiscal `FY26 Q1`; quarter mapping is approved by `repo_config` with explicit reason text.
- Territory bundles: `9`; forward fallback count `4`.
- DirectorBundles: `9`; unsupported datasets are explicitly optional-empty.
- Publish gate: `ok`; `27` PI list views audited; `0` list-view findings; `0` total findings.
- Truth packet: `ok`; `9` directors, `60` metrics, `60` claims, `0` blockers, `0` tie-out mismatches.
- Deck audit: `ok`; deck at `output/source_backed_decks/2026-04-30/live-all-sources-pipeline-open-v18/source_backed_monthly_review.pptx`.
- Polish audit: `ok`; `43` deterministic replacements, `0` findings.
- Table contract audit: `ok`; `5` standard tables, exact headers/styles, `0` findings.
- Semantic audit: `ok`; human-style score `100`, `0` findings.
- Render audit: `ok`; montage at `output/source_backed_deck_renders/2026-04-30/live-all-sources-pipeline-open-v18/montage.png`.
- Release bundle: `ok`; `21` required artifacts copied, `0` missing, zip size `452875` bytes, SharePoint handoff `upload_ready=true`.
- SharePoint upload plan: `planned`; `5` publish assets, `0` missing, folder `General/Book of Business/Sales Director Reporting/Q2 2026/Sales Director Monthly Review - 2026-04-30 - live-all-sources-pipeline-open-v18`.
- SharePoint upload result: `ok`; `5` uploaded, `0` skipped, all `single_put` into the v18 run folder.

## Orchestrator Integration Note

Add this as a new source-backed subcommand or preflight lane beside `scripts/run_sales_director_monthly_cadence.py`, not as hidden behavior inside the legacy Office builder. The runner should write one status packet plus source-backed latest aliases with each command, return code, JSON status, output path, and blocking reason; it should not mutate production dashboards or edit builder/test files.

## Claude/Codex Handoff

- One owner per file: the worker assigned to a script edits that script only; the doc worker owns only this plan.
- Before editing, run `git status --short` and treat all unrelated modified/untracked files as parallel work.
- Codex should run and wire deterministic CLI gates; Claude can review manifests, narrative, and deck/truth alignment from artifacts.
- Handoffs should name exact artifact paths and current status fields, not paraphrased claims.
- Do not duplicate edits: if a peer is changing the runner, review the diff or write docs against it; do not independently patch the same runner file.
