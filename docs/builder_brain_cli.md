# Builder Brain CLI

`scripts/builder_brain.py` is the builder-focused layer for planning analytics assets across:

- Salesforce reports
- native Salesforce dashboards
- CRM Analytics dashboards

It is intentionally upstream of any real asset mutation. The goal is to produce a better build baseline before any dashboard JSON, Wave PATCH work, or report wiring happens.

## Commands

### `validate`

Checks the builder-brain profile config.

```bash
python3 scripts/builder_brain.py validate --json
```

### `inventory`

Lists supported surface adapters, build modes, critique checks, excellence patterns, and local exemplar surfaces.

```bash
python3 scripts/builder_brain.py inventory --json
```

### `spec`

Builds a neutral build spec from a business ask. The spec chooses:

- primary surface
- optional secondary handoff surface
- build mode
- excellence target: either a named local exemplar or a reusable local pattern
- recommended filters
- metric roles
- review gates

```bash
python3 scripts/builder_brain.py spec \
  --query 'Manager owner list report for renewals needing follow-up this week' \
  --json
```

### `retrieve`

Retrieves the best local patterns and exemplar surfaces before drafting.

This is the explicit “exemplar-first” step. It ranks local gold-standard patterns and surfaces so the builder does not start from a blank generic template.

```bash
python3 scripts/builder_brain.py retrieve \
  --query 'Executive commercial rhythm view for ownership alignment, handoff quality, and renewal semantic confidence' \
  --json
```

### `draft`

Builds a first-pass surface draft from the neutral spec.

The draft now carries an explicit `page_model` informed by the excellence library, so a manager operating system and an executive truth surface do not collapse into the same generic layout.

The draft also now carries `design_cues`, including:

- top retrieved patterns
- top retrieved exemplars
- local carry-forward cues
- widget shapes to avoid by persona/question

Examples:

```bash
python3 scripts/builder_brain.py draft \
  --query 'Executive commercial rhythm view for ownership alignment, handoff quality, and renewal semantic confidence' \
  --json
```

```bash
python3 scripts/builder_brain.py draft \
  --query 'Manager owner list report for renewals needing follow-up this week' \
  --json
```

### `critique`

Critiques the first-pass draft. This is the CLI expression of the builder-brain rule that the baseline is not final.

The critique output now includes named specialist reviews:

- `surface_planner`
- `story_critic`
- `action_critic`
- `visual_critic`

This is still deterministic, but it pushes the builder brain toward a multi-critic shape instead of one generic pass.

```bash
python3 scripts/builder_brain.py critique \
  --query 'Executive commercial rhythm view for ownership alignment, handoff quality, and renewal semantic confidence' \
  --surface salesforce_report \
  --json
```

### `revise`

Builds a second-pass draft from the critique output.

`revise` is the first real builder-brain improvement loop:

- it can correct the surface back to the builder default when the forced surface is clearly wrong
- it upgrades generic metric labels into actual / variance / risk / action roles
- it adds the expected handoff surface when the local excellence target requires one
- it emits a `page_blueprint` so the output is organized around business purpose, not just layout slots
- it reruns critique and returns both the before/after state

Examples:

```bash
python3 scripts/builder_brain.py revise \
  --query 'Executive commercial rhythm view for ownership alignment, handoff quality, and renewal semantic confidence' \
  --surface salesforce_report \
  --json
```

```bash
python3 scripts/builder_brain.py revise \
  --query 'Manager owner list report for renewals needing follow-up this week' \
  --json
```

The JSON output includes:

- `spec`: the first-pass neutral build spec
- `draft`: the first-pass baseline draft
- `critique_before`: the pre-revision critique with score and verdict
- `revised_spec`: the corrected build spec
- `revised_draft`: the second-pass draft, marked `critic_revised`
- `critique_after`: the second critique pass; `ready_for_build` means the draft cleared the current local checks
- `revisions`: the explicit change log applied by the builder brain

### `package`

Builds an execution handoff package from the revised draft.

This is the bridge between the builder brain and the downstream execution layer. It does not mutate assets itself. It packages the improved design into:

- `build_brief`: audience, decision, build mode, excellence target, reference exemplar
- `surface_contract`: the concrete surface payload to build
- `surface_contract.handoff_target`: the typed follow-up target, including `destination_type` and any resolved id/label
- `critic_rationale`: which specialist critics drove which revisions and constraints
- `design_constraints`: the must-keep constraints the executor should not violate
- `execution_plan`: a phase-by-phase implementation skeleton for the chosen surface
- `review_gates`: the gates the final surface still needs to satisfy
- `acceptance_criteria`: what “done well” means for this package
- `next_steps`: the immediate executor instructions

Examples:

```bash
python3 scripts/builder_brain.py package \
  --query 'Executive commercial rhythm view for ownership alignment, handoff quality, and renewal semantic confidence' \
  --surface salesforce_report \
  --json
```

```bash
python3 scripts/builder_brain.py package \
  --query 'Manager owner list report for renewals needing follow-up this week' \
  --json
```

Key output fields:

- `build_package.package_status`: `ready_for_execution` or `needs_more_revision`
- `build_package.execution_lane`: where the executor should run next
- `build_package.repo_execution_fit`: whether this repo is a strong or partial fit for execution
- `build_package.surface_contract`: the actual report/dashboard handoff structure
- `build_package.surface_contract.handoff_target`: the typed follow-up target contract
- `build_package.critic_rationale`: why the package is shaped the way it is
- `build_package.design_constraints`: non-negotiable design constraints for execution
- `build_package.execution_plan`: the phase-by-phase implementation plan
- `build_package.next_steps`: the first executor actions

### `handoff`

Builds an executor-facing wrapper around the package and writes the package artifact to disk.

`handoff` is the thin bridge between the builder brain and the real execution lane:

- it writes `build_package.json` to an output directory
- it also writes `execution_plan.json` so the executor has a phase-by-phase plan on disk
- it identifies the primary execution lane
- it names the exact repo commands we can actually stand behind
- it carries the specialist-critic constraints forward so the executor can see why each guardrail exists
- it stays honest about partial-fit lanes like native Salesforce reports and native dashboards

Examples:

```bash
python3 scripts/builder_brain.py handoff \
  --query 'Executive commercial rhythm view for ownership alignment, handoff quality, and renewal semantic confidence' \
  --surface salesforce_report \
  --output-dir output/builder_brain/commercial_rhythm_control_tower \
  --json
```

```bash
python3 scripts/builder_brain.py handoff \
  --query 'Manager owner list report for renewals needing follow-up this week' \
  --output-dir output/builder_brain/owner_list_report \
  --json
```

Key output fields:

- `executor_handoff.primary_lane`: the next execution lane
- `executor_handoff.package_artifact`: the emitted local package JSON
- `executor_handoff.execution_plan_artifact`: the emitted implementation-plan JSON
- `executor_handoff.design_constraints`: the must-keep design constraints for the executor
- `executor_handoff.critic_rationale`: the critic-by-critic rationale behind the package
- `executor_handoff.execution_plan`: the in-memory phase-by-phase plan
- `executor_handoff.available_commands`: exact repo commands available from this repo
- `executor_handoff.external_steps`: explicit non-repo work that still needs to happen

For native Salesforce surfaces, `handoff` now points to concrete authoring executors instead of stopping at prose-only external guidance:

- `scripts/salesforce_report_executor.py validate|bundle|preview|verify|apply|complete|delete`
- `scripts/salesforce_dashboard_executor.py validate|bundle|preview|verify|apply|complete|delete`

### `probe`

Builds the handoff, runs the native executor `complete` flow, and can optionally clean up the probe asset, all from one top-level builder command.

`probe` is the highest-level mutating builder wrapper for native Salesforce surfaces:

- it writes the same `build_package.json` and `execution_plan.json` artifacts as `handoff`
- it runs the downstream native executor with live org context
- it returns the created asset id in one top-level result
- with `--cleanup`, it also runs the matching executor `delete` command and confirms the probe asset is gone
- with `--package`, it reuses an existing `build_package.json` instead of rebuilding from query routing

Examples:

```bash
python3 scripts/builder_brain.py probe \
  --query 'Manager owner list report for renewals needing follow-up this week' \
  --target-org apro@simcorp.com \
  --clone-from-report-id 00OTb000008TZaTMAW \
  --cleanup \
  --output-dir output/builder_brain/report_probe_live \
  --json
```

```bash
python3 scripts/builder_brain.py probe \
  --query 'Native dashboard headline rollup for manager forecast inspection' \
  --target-org apro@simcorp.com \
  --clone-from-dashboard-id 01ZTb00000DoGYLMA3 \
  --session dashboard_probe_session \
  --dashboard-filter-automation-script scripts/salesforce_dashboard_filter_automation.py \
  --cleanup \
  --output-dir output/builder_brain/dashboard_probe_live \
  --json
```

```bash
python3 scripts/builder_brain.py probe \
  --package output/builder_brain/dashboard_executor_live_preview_20260327/build_package.json \
  --target-org apro@simcorp.com \
  --session dashboard_probe_session \
  --executor-timeout-seconds 900 \
  --dashboard-filter-automation-script scripts/salesforce_dashboard_filter_automation.py \
  --cleanup \
  --output-dir output/builder_brain/dashboard_probe_from_package \
  --json
```

Key output fields:

- `summary.primary_lane`: which native execution lane ran
- `summary.created_asset_id`: the live report/dashboard id created by the probe
- `summary.cleanup_requested`: whether the probe also deleted the asset
- `summary.cleanup_status`: the delete result when cleanup ran
- `summary.package_source`: whether the probe used `query_routing` or a `provided_package`
- `execution_result`: the nested native executor `complete` payload
- `cleanup_result`: the nested native executor `delete` payload when cleanup ran

### `probe-matrix`

Runs a manifest of top-level native probes and writes one `probe_result.json` per entry plus one matrix summary.

- use this when you want to regression-test multiple saved `build_package.json` artifacts from one command
- each manifest entry can point to a `package` or a `query`
- each entry can override `target_org`, `cleanup`, `session`, and clone ids

Example manifest:

```json
{
  "defaults": {
    "target_org": "apro@simcorp.com",
    "cleanup": true
  },
  "probes": [
    {
      "name": "manager_report_probe",
      "package": "output/.../report/build_package.json",
      "clone_from_report_id": "00OTb000008TZaTMAW"
    },
    {
      "name": "manager_dashboard_probe",
      "package": "output/.../dashboard/build_package.json",
      "clone_from_dashboard_id": "01ZTb00000CyYpVMAV",
      "session": "dashboard_probe_matrix_session"
    }
  ]
}
```

```bash
python3 scripts/builder_brain.py probe-matrix \
  --manifest output/.../probe_matrix_manifest.json \
  --executor-timeout-seconds 900 \
  --dashboard-filter-automation-script scripts/salesforce_dashboard_filter_automation.py \
  --output-dir output/builder_brain/probe_matrix_live \
  --json
```

Checked-in live smoke suite:

- report-only manifest: `config/builder_brain_live_smoke/probe_matrix_report_live_smoke.json`
- mixed report + dashboard manifest: `config/builder_brain_live_smoke/probe_matrix_mixed_live_smoke.json`
- manifests now resolve relative `package` and `dashboard_filter_automation_script` paths from the manifest directory, so the suite stays portable
- recurring smoke wrappers:
  - `make builder-brain-live-smoke-report`
  - `make builder-brain-live-smoke`

Optional overrides for the `make` targets:

- `BUILDER_BRAIN_LIVE_SMOKE_TARGET_ORG`
- `BUILDER_BRAIN_LIVE_SMOKE_TIMEOUT`
- `BUILDER_BRAIN_LIVE_SMOKE_OUTPUT_DIR`

Key output fields:

- `summary.completed`: number of probe entries that actually ran
- `summary.ok_count` / `summary.warn_count` / `summary.error_count`: batch status counts
- `summary.cleanup_requested_count`: how many entries requested cleanup
- `probe_runs[].result_path`: path to the full per-entry `probe_result.json`

## Native Salesforce Capability Notes

As of March 27, 2026, the live org capability split is:

- Metadata API access is blocked for this user.
- native Salesforce reports are still accessible through the Reports and Dashboards REST API.
- native Salesforce dashboards are also accessible through the Dashboards REST API.

Low-risk org probes that established this:

```bash
sf org list metadata --metadata-type Report --folder 'COO Run The Business' --target-org apro@simcorp.com --json
sf project retrieve start --metadata Report --target-org apro@simcorp.com --json --wait 1
sf project retrieve start --metadata Dashboard --target-org apro@simcorp.com --json --wait 1
sf api request rest '/services/data/v66.0/sobjects/Report/describe' --target-org apro@simcorp.com | jq '{name, createable, updateable, deletable, retrieveable, queryable}'
sf api request rest '/services/data/v66.0/sobjects/Dashboard/describe' --target-org apro@simcorp.com | jq '{name, createable, updateable, deletable, retrieveable, queryable}'
sf api request rest '/services/data/v66.0/analytics/reports' --target-org apro@simcorp.com
sf api request rest '/services/data/v66.0/analytics/reports/00OTb000008TZaTMAW/describe' --target-org apro@simcorp.com
sf api request rest '/services/data/v66.0/analytics/dashboards' --target-org apro@simcorp.com
```

What those probes mean:

- `sf org list metadata` and `sf project retrieve start` both fail with `INSUFFICIENT_ACCESS: use of the Metadata API requires a user with the ModifyAllData or ModifyMetadata permissions`.
- the standard `Report` and `Dashboard` sObjects are `queryable` and `retrieveable`, but both are `createable: false` and `updateable: false`, so normal sObject CRUD is not the authoring path.
- the org does allow report list/describe reads through `/services/data/v66.0/analytics/reports/...`.
- the org also allows dashboard list reads through `/services/data/v66.0/analytics/dashboards`.
- the official Reports and Dashboards REST API guide documents:
  - report create with `POST /analytics/reports`
  - report save with `PATCH /analytics/reports/{id}`
  - report clone with `POST /analytics/reports?cloneId=...`
  - dashboard save with `PATCH /analytics/dashboards/{id}`
  - dashboard clone with `POST /analytics/dashboards?cloneId=...`

Builder implication:

- both native executors can now produce REST preview contracts without depending on Metadata API.
- the report executor now also has an `apply` lane; after live autofill and semantic cleanup, its dry-run apply contract is down to clone-response substitution only.
- the dashboard executor now also has an `apply` lane; after autofill and manual filter-intent cleanup, its dry-run apply contract is down to clone-response substitution only.

### `builder_brain_handoff_targets.py`

The typed `surface_contract.handoff_target` contract is backed by
`config/builder_brain_handoff_targets.json`. Use the dedicated harness to
inspect that registry before changing builder logic:

```bash
python3 scripts/builder_brain_handoff_targets.py validate --json
python3 scripts/builder_brain_handoff_targets.py inventory --unresolved-only --json
python3 scripts/builder_brain_handoff_targets.py resolve \
  --source-surface-id commercial_rhythm_control_tower \
  --target-surface-type salesforce_report \
  --check-live \
  --describe-top 2 \
  --json
```

That harness keeps report/dashboard handoff ids out of the builder brain itself:

- `validate` checks registry structure, destination-type consistency, and optional live ids
- `inventory` shows which source surfaces are already wired and which are still unresolved
- unresolved entries can still carry `preferred_search_terms`, so the registry can model a known follow-up lane before the exact report id is confirmed
- `resolve` returns the current mapping for one source surface, ranks live Salesforce report candidates, and emits `suggested_target` when the registry still only has discovery hints
- `resolve --describe-top N` also attaches lightweight live report fingerprints such as format, filter count, and visible detail-column preview for the top candidates

When the resolver still leaves two plausible report choices, use `compare` to evaluate the finalists side by side:

```bash
python3 scripts/builder_brain_handoff_targets.py compare \
  --source-surface-id commercial_rhythm_control_tower \
  --report-id 00OTb000008TZaTMAW \
  --report-id 00OTb000008TZsDMAW \
  --json
```

That compares the chosen reports on:

- ranking score in the current builder-brain context
- recency signals like `LastViewedDate` and `LastRunDate`
- lightweight report fingerprints such as `report_format`, filter count, and visible detail-column preview
- builder-fit dimensions such as `owner_accountability_fit`, `handoff_complementarity`, `field_filter_alignment`, and `executive_story_substitution_risk`

### `salesforce_report_executor.py`

The report executor turns a `build_package.json` into a native Salesforce report authoring bundle.

```bash
python3 scripts/salesforce_report_executor.py validate \
  --package output/.../build_package.json \
  --json

python3 scripts/salesforce_report_executor.py bundle \
  --package output/.../build_package.json \
  --output-dir output/.../salesforce_report_bundle \
  --json

python3 scripts/salesforce_report_executor.py preview \
  --package output/.../build_package.json \
  --clone-from-report-id 00OTb000008TZaTMAW \
  --autofill-live \
  --target-org apro@simcorp.com \
  --output-dir output/.../salesforce_report_preview \
  --json

python3 scripts/salesforce_report_executor.py verify \
  --package output/.../build_package.json \
  --report-id 00OTb000008TZaTMAW \
  --clone-from-report-id 00OTb000008TZaTMAW \
  --autofill-live \
  --target-org apro@simcorp.com \
  --output-dir output/.../salesforce_report_verify \
  --json

python3 scripts/salesforce_report_executor.py apply \
  --package output/.../build_package.json \
  --clone-from-report-id 00OTb000008TZaTMAW \
  --autofill-live \
  --target-org apro@simcorp.com \
  --output-dir output/.../salesforce_report_apply_preview \
  --json

python3 scripts/salesforce_report_executor.py complete \
  --package output/.../build_package.json \
  --clone-from-report-id 00OTb000008TZaTMAW \
  --autofill-live \
  --target-org apro@simcorp.com \
  --output-dir output/.../salesforce_report_complete \
  --json

python3 scripts/salesforce_report_executor.py delete \
  --report-id 00OTb000008dPjVMAU \
  --target-org apro@simcorp.com \
  --output-dir output/.../salesforce_report_delete \
  --json
```

It emits:

- `salesforce_report_bundle.json`
- `salesforce_report_definition.json`
- `salesforce_report_validation_checklist.json`
- `salesforce_report_rest_preview.json`
- `salesforce_report_fill_requirements.json`
- `salesforce_report_autofill_summary.json`
- `salesforce_report_verify.json`
- `salesforce_report_apply_preview.json`

The bundle keeps the report CLI-first and concrete:

- format, grouping, columns, filters, and sort order
- field contract for grouping/display/filter/sort fields
- validation checklist and acceptance criteria
- typed handoff target when the report should resolve into CRMA

The preview layer is the last-mile contract between the builder package and the Reports REST API:

- it compiles the exact REST path and method sequence
- it chooses `create_new`, `patch_existing`, or `clone_then_patch`
- it writes a fill-first request body when report field API names are still unresolved
- it emits explicit fill requirements only for concrete REST blockers that still need a live value or field mapping
- it can reduce those fill requirements with either:
  - `--baseline-report-describe-json <path>` for local report-describe evidence
  - `--autofill-live --target-org apro@simcorp.com` for live report-describe evidence
- it also consults [native_surface_autofill_vocab.json](/Users/test/crm-analytics/config/native_surface_autofill_vocab.json) for high-confidence builder-to-Salesforce field aliases when the builder term is not the same as the live report label
- it now emits a real `sortBy` payload only for native-report-safe sorts, preserves unsupported semantic sort intent outside the REST body, and carries manual filter/semantic-column intent outside the REST payload when the package does not define a concrete native-report value or field

The apply layer sits one step above preview:

- it reuses the same packaged REST contract
- it classifies unresolved requirements into:
  - external blockers that still need human input
  - internal clone-response substitution that the executor can satisfy itself when a real clone-then-patch path is still required
- `apply` without `--apply` is a dry-run readiness check
- when the preview started from `clone_then_patch` only to harvest live describe context, `apply` now promotes the execution strategy to `create_new`
- `apply --apply` executes the effective strategy, not just the preview seed strategy

The verify layer closes the loop after authoring or apply:

- it recompiles the expected report contract from the builder package
- it compares a live or local report describe payload back to that packaged contract
- it treats preserved manual filter/detail/sort intent as allowed extra live structure instead of false mismatches
- it emits `salesforce_report_verify.json` with expected contract, actual contract, and findings

The `complete` layer sits one step above `apply`:

- it runs `apply --apply` first
- it then runs `verify` against the created live report id
- it returns one aggregated result with:
  - the applied report id
  - the apply status
  - the verify status

The `delete` layer is the probe-cleanup path:

- it sends `DELETE /analytics/reports/{id}`
- it then retries report `describe` until the report returns `NOT_FOUND` / `ENTITY_IS_DELETED`
- it emits:
  - `salesforce_report_delete_response.json`
  - `salesforce_report_delete_verify.json`

Live proof from March 27, 2026:

- `clone_then_patch` preview against report `00OTb000008TZaTMAW`
- unresolved fill requirements dropped from `26` to `1`
- safe autofills included folder id, report type, 2 grouping fields, and 4 native-report-safe detail columns
- grouped fields are now omitted from `detailColumns`, so `Owner` stays in `groupingsDown` and no longer pollutes the summary-report detail payload
- the unsupported `Action Queue: Handoff Quality` sort and grouped `Owner` sort now stay as explicit manual intent in `omitted_sort_intents` instead of polluting the Reports REST payload
- the 4 packaged report filters now stay in `manual_filter_intents` because the builder package names the filter vocabulary but not fixed native-report values
- the 2 unsupported semantic columns now stay in `manual_detail_intents` instead of polluting `detailColumns`
- `apply` dry-run now returns `apply_ready: true` with `external_fill_requirement_count: 0` and promotes the effective strategy to `create_new`
- a live probe create through `salesforce_report_executor.py apply --apply ...` succeeded for report `00OTb000008dPjVMAU`
- a follow-on `verify` run against `00OTb000008dPjVMAU` returned `finding_count: 0`
- `salesforce_report_executor.py delete` now handles probe cleanup directly and retries until the report no longer resolves from `describe`
- the probe report was deleted after verification, so the org was left clean

### `salesforce_dashboard_executor.py`

The dashboard executor turns a `build_package.json` into a native Salesforce dashboard authoring bundle.

```bash
python3 scripts/salesforce_dashboard_executor.py validate \
  --package output/.../build_package.json \
  --json

python3 scripts/salesforce_dashboard_executor.py bundle \
  --package output/.../build_package.json \
  --output-dir output/.../salesforce_dashboard_bundle \
  --json

python3 scripts/salesforce_dashboard_executor.py preview \
  --package output/.../build_package.json \
  --autofill-live \
  --target-org apro@simcorp.com \
  --output-dir output/.../salesforce_dashboard_preview \
  --json

python3 scripts/salesforce_dashboard_executor.py verify \
  --package output/.../build_package.json \
  --dashboard-id 01ZTb00000DoGYLMA3 \
  --manual-filter-authoring-json output/.../salesforce_dashboard_manual_filter_authoring.json \
  --autofill-live \
  --target-org apro@simcorp.com \
  --output-dir output/.../salesforce_dashboard_verify \
  --json

python3 scripts/salesforce_dashboard_executor.py apply \
  --package output/.../build_package.json \
  --autofill-live \
  --target-org apro@simcorp.com \
  --output-dir output/.../salesforce_dashboard_apply_preview \
  --json

python3 scripts/salesforce_dashboard_executor.py complete \
  --package output/.../build_package.json \
  --autofill-live \
  --target-org apro@simcorp.com \
  --session dashboard_complete_session \
  --output-dir output/.../salesforce_dashboard_complete \
  --json

python3 scripts/salesforce_dashboard_executor.py delete \
  --dashboard-id 01ZTb00000FDzElMAL \
  --target-org apro@simcorp.com \
  --output-dir output/.../salesforce_dashboard_delete \
  --json
```

It emits:

- `salesforce_dashboard_bundle.json`
- `salesforce_dashboard_definition.json`
- `salesforce_dashboard_component_plan.json`
- `salesforce_dashboard_rest_preview.json`
- `salesforce_dashboard_fill_requirements.json`
- `salesforce_dashboard_autofill_summary.json`
- `salesforce_dashboard_verify.json`
- `salesforce_dashboard_apply_preview.json`

The native dashboard bundle stays lightweight by design:

- page/view model and filters
- component plan derived from the packaged storyboard
- explicit backing report dependencies
- validation checklist and typed handoff target

The verify layer closes the loop after authoring or apply:

- it recompiles the expected dashboard contract from the builder package
- it compares a live or local dashboard payload back to that packaged contract
- it can load `salesforce_dashboard_manual_filter_authoring.json` and verify that manually authored native filters actually match the validated post-clone filter contract
- that manual-filter artifact can also be reduced to a smaller authored subset when you are validating incremental post-clone filter work instead of the full proposed filter set
- without that artifact, it still falls back to the package-derived manual filter intents
- it emits `salesforce_dashboard_verify.json` with expected contract, actual contract, and findings

The preview layer turns that bundle into a Dashboards REST contract:

- it chooses `patch_existing` or `clone_then_patch`
- it emits the clone request for a baseline dashboard when needed
- it emits the PATCH body as top-level dashboard metadata with native `layout.components` grid structure, matching the live save contract
- it makes unresolved report ids, folder ids, and cloned dashboard ids explicit before any live mutation
- it can reduce those fill requirements with either:
  - `--baseline-dashboard-json <path>` for local dashboard metadata
  - `--autofill-live --target-org apro@simcorp.com` for live dashboard metadata
- it also consults [native_surface_autofill_vocab.json](/Users/test/crm-analytics/config/native_surface_autofill_vocab.json) for scoped component-to-report mappings when the baseline dashboard does not expose an exact title match
- it can also resolve a package-scoped clone baseline automatically from repo vocab when no explicit `--clone-from-dashboard-id` is supplied
- component matching now allows unique partial-title matches, so packaged metrics like `Actual Forecast & Closed Won` can resolve against live headers like `Forecast & Closed Won`
- it now also tries to inherit compatible component `properties` from the baseline dashboard or repo vocab so the save contract includes real aggregates/groupings when the baseline exposes them
- unresolved native dashboard filter vocabulary now stays in `manual_filter_intents` instead of polluting the REST payload with empty option contracts
- repo-scoped filter templates can now carry proposed native filter contracts plus live `filteroptionsanalysis` proof, so the executor distinguishes between:
  - filters that are valid in principle for the target reports
  - filters that are actually PATCH-ready from the chosen clone baseline
- when manual filter authoring is still required, preview/apply now write `salesforce_dashboard_manual_filter_authoring.json` with:
  - the resolved clone baseline
  - baseline filter count
  - validated proposed filter contracts
  - explicit post-clone authoring steps
- preview/apply also now write `salesforce_dashboard_filter_playbook.json` plus a readable `salesforce_dashboard_filter_playbook.md` companion, so the last UI step is an explicit per-filter sequence instead of only raw JSON
- preview/apply also now write `salesforce_dashboard_filter_automation_plan.json`, which adds browser-friendly edit-route context, per-filter automation actions, and a verify command template for the same manual-filter contract
- when live org reads are available, preview/apply also write `salesforce_dashboard_baseline_candidates.json` with ranked alternative clone baselines that already carry native filters
- those candidate artifacts now also include a `baseline_strategy` judgment, so preview can explicitly tell you when the current lightweight baseline is still the right choice and the alternatives are only useful as filter references
- unresolved component property contracts now stay as explicit external blockers instead of letting `apply` overclaim readiness

The apply layer sits one step above preview:

- it reuses the same packaged REST contract
- it classifies unresolved requirements into:
  - external blockers that still need human input
  - internal clone-response substitution that the executor can satisfy itself
- `apply` without `--apply` is a dry-run readiness check
- `apply --apply` executes the clone then patch sequence when no external blockers remain

The `complete` layer sits one step above `apply`:

- it runs `apply --apply` first and captures the emitted manual filter automation plan
- if manual native filters are still required, it runs `salesforce_dashboard_filter_automation.py run-filter-flow --all-filters --through verify-dashboard`
- it returns one aggregated result with:
  - the live authored dashboard id
  - the filter-flow status
- the manual-filter verification count
- when the package does not need browser-authored native filters, it falls back to a direct `verify` after apply

The `delete` layer is the probe-cleanup path:

- it sends `DELETE /analytics/dashboards/{id}`
- it then retries dashboard reads until the dashboard returns `NOT_FOUND` / `ENTITY_IS_DELETED`
- it emits:
  - `salesforce_dashboard_delete_response.json`
  - `salesforce_dashboard_delete_verify.json`

Live proof from March 27, 2026:

- `clone_then_patch` preview now auto-resolves the clone-safe baseline `Operational Work Done` (`01ZTb00000CyYpVMAV`)
- dry-run `apply` now returns `apply_ready: true` with `external_fill_requirement_count: 0`
- safe autofills now include folder id, 3 backing report ids, and 3 compatible component property contracts
- live preview for the packaged manager dashboard now preserves 3 validated manual filter intents outside the REST payload:
  - `Close Date` quarter ranges
  - `Opportunity Owner`
  - `Forecast Category`
- the preview/apply output now includes a reusable manual filter authoring artifact instead of only inline warnings
- the preview/apply output now also includes a per-filter playbook artifact that can drive browser/operator follow-through and later `verify` runs
- the preview/apply output now also includes a machine-consumable automation-plan artifact for the same post-clone filter flow
- the live dashboard apply succeeded through `salesforce_dashboard_executor.py apply --apply`, creating dashboard `01ZTb00000FDzElMAL`
- live `verify` against that authored dashboard returned `finding_count: 0`
- `salesforce_dashboard_executor.py delete` now handles probe cleanup directly and retries until the dashboard no longer resolves from the REST endpoint
- the probe dashboard was then deleted and confirmed removed from the org
- dashboard filter creation is still not treated as PATCH-ready from the lightweight baseline, even when Salesforce validates the proposed filter values against the target report fields

### `salesforce_dashboard_filter_automation.py`

The dashboard filter automation helper consumes
`salesforce_dashboard_filter_automation_plan.json` and turns it into a
repeatable browser-prep step.

```bash
python3 scripts/salesforce_dashboard_filter_automation.py validate \
  --plan output/.../salesforce_dashboard_filter_automation_plan.json \
  --json

python3 scripts/salesforce_dashboard_filter_automation.py prepare \
  --plan output/.../salesforce_dashboard_filter_automation_plan.json \
  --session dashfilter \
  --target-org apro@simcorp.com \
  --dashboard-id 01ZTb00000FE46MMAT \
  --output-dir output/.../salesforce_dashboard_filter_prepare \
  --json

python3 scripts/salesforce_dashboard_filter_automation.py open-filter \
  --prepare-artifact output/.../salesforce_dashboard_filter_prepare.json \
  --plan output/.../salesforce_dashboard_filter_automation_plan.json \
  --session dashfilter \
  --output-dir output/.../salesforce_dashboard_filter_open \
  --json

python3 scripts/salesforce_dashboard_filter_automation.py open-filter-field \
  --open-filter-artifact output/.../salesforce_dashboard_filter_open.json \
  --plan output/.../salesforce_dashboard_filter_automation_plan.json \
  --session dashfilter \
  --output-dir output/.../salesforce_dashboard_filter_field \
  --json

python3 scripts/salesforce_dashboard_filter_automation.py open-filter-value \
  --field-artifact output/.../salesforce_dashboard_filter_field.json \
  --plan output/.../salesforce_dashboard_filter_automation_plan.json \
  --filter-name "Forecast Category" \
  --session dashfilter \
  --output-dir output/.../salesforce_dashboard_filter_value \
  --json

python3 scripts/salesforce_dashboard_filter_automation.py select-filter-option \
  --value-artifact output/.../salesforce_dashboard_filter_value.json \
  --plan output/.../salesforce_dashboard_filter_automation_plan.json \
  --filter-name "Forecast Category" \
  --option-alias "Pipeline" \
  --session dashfilter \
  --output-dir output/.../salesforce_dashboard_filter_option \
  --json

python3 scripts/salesforce_dashboard_filter_automation.py apply-filter-value \
  --option-artifact output/.../salesforce_dashboard_filter_option.json \
  --session dashfilter \
  --output-dir output/.../salesforce_dashboard_filter_apply \
  --json

python3 scripts/salesforce_dashboard_filter_automation.py commit-dashboard-filter \
  --apply-artifact output/.../salesforce_dashboard_filter_apply.json \
  --session dashfilter \
  --output-dir output/.../salesforce_dashboard_filter_commit \
  --json

python3 scripts/salesforce_dashboard_filter_automation.py save-dashboard \
  --commit-artifact output/.../salesforce_dashboard_filter_commit.json \
  --session dashfilter \
  --output-dir output/.../salesforce_dashboard_filter_save \
  --json

python3 scripts/salesforce_dashboard_filter_automation.py run-filter-flow \
  --plan output/.../salesforce_dashboard_filter_automation_plan.json \
  --filter-name "Forecast Category" \
  --option-alias "Pipeline" \
  --through verify-dashboard \
  --verify-package output/.../build_package.json \
  --session dashfilter \
  --output-dir output/.../salesforce_dashboard_filter_flow \
  --json

python3 scripts/salesforce_dashboard_filter_automation.py run-filter-flow \
  --plan output/.../salesforce_dashboard_filter_automation_plan.json \
  --all-filters \
  --through verify-dashboard \
  --verify-package output/.../build_package.json \
  --session dashfilter \
  --output-dir output/.../salesforce_dashboard_filter_flow_all \
  --json
```

`prepare` is intentionally read-only from the org perspective:

- it opens the dashboard edit route in a Playwright session
- captures a fresh editor snapshot
- extracts candidate refs for editor controls like `Add filter`, `Save`, and `Done`
- detects blocking editor signals such as deleted/inaccessible dashboard state
- writes:
  - `salesforce_dashboard_filter_prepare.json`
  - `salesforce_dashboard_filter_prepare_snapshot.yml`

`open-filter` sits one step above `prepare`:

- it consumes `salesforce_dashboard_filter_prepare.json`
- checks whether the prepared `Add filter` ref is actionable
- surfaces any blocking signals already recorded by `prepare`
- warns instead of clicking when the editor state shows `Add filter` disabled
- otherwise clicks into the filter picker and writes:
  - `salesforce_dashboard_filter_open.json`
  - `salesforce_dashboard_filter_open_snapshot.yml`

`open-filter-field` sits one step above `open-filter`:

- it consumes `salesforce_dashboard_filter_open.json`
- resolves the intended field from the automation plan or `--filter-name`
- prefers exact option matches inside the picker over background dashboard text
- clicks the selected field and writes:
  - `salesforce_dashboard_filter_field.json`
  - `salesforce_dashboard_filter_field_snapshot.yml`

`open-filter-value` sits one step above `open-filter-field`:

- it consumes `salesforce_dashboard_filter_field.json`
- finds the real `Add Filter Value` control instead of background `Add` buttons
- clicks into the value picker and writes:
  - `salesforce_dashboard_filter_value.json`
  - `salesforce_dashboard_filter_value_snapshot.yml`

`select-filter-option` sits one step above `open-filter-value`:

- it consumes `salesforce_dashboard_filter_value.json`
- prefers exact option matches over background chart text
- clicks the target option and writes:
  - `salesforce_dashboard_filter_option.json`
  - `salesforce_dashboard_filter_option_snapshot.yml`

`apply-filter-value` sits one step above `select-filter-option`:

- it consumes `salesforce_dashboard_filter_option.json`
- clicks the dialog-level `Apply` control
- snapshots whether the top-level dashboard `Add` action is now ready
- writes:
  - `salesforce_dashboard_filter_apply.json`
  - `salesforce_dashboard_filter_apply_snapshot.yml`

`commit-dashboard-filter` sits one step above `apply-filter-value`:

- it consumes `salesforce_dashboard_filter_apply.json`
- clicks the dashboard-level `Add` button once the filter value is ready
- snapshots whether the editor `Save` action is now ready
- writes:
  - `salesforce_dashboard_filter_commit.json`
  - `salesforce_dashboard_filter_commit_snapshot.yml`

`save-dashboard` sits one step above `commit-dashboard-filter`:

- it consumes `salesforce_dashboard_filter_commit.json`
- clicks the dashboard `Save` action once the native filter is committed
- snapshots the post-save editor state and whether `Done` is ready
- writes:
  - `salesforce_dashboard_filter_save.json`
  - `salesforce_dashboard_filter_save_snapshot.yml`

`run-filter-flow` sits above the individual browser helper commands:

- it composes the full native filter authoring sequence in one CLI call
- it writes stage-specific artifacts under numbered subdirectories
- `--through` lets you stop at any boundary from `prepare` to `verify-dashboard`
- use `--through apply-filter-value` for a read-only-ish progression and `--through save-dashboard` when you want the full saved filter flow
- use `--through verify-dashboard --verify-package ...` when you want the closed loop:
  save the filter, generate a one-filter manual verification contract, and immediately run `salesforce_dashboard_executor.py verify`
- use `--all-filters --through verify-dashboard --verify-package ...` when you want the flow to author every planned native filter in sequence and then verify the full authored filter set
- the verify stage writes:
  - `09_verify_dashboard/salesforce_dashboard_manual_filter_verification.json`
  - `09_verify_dashboard/salesforce_dashboard_verify.json`

That helper is the bridge between the machine-consumable automation plan and an
actual browser session. It still does not author filter values by itself, but
it now gets the workflow to a concrete prepared editor state, through the
filter picker, into the field-specific configuration state, and then up to the
value-selection boundary, the post-selection commit state, and the point where
the dashboard editor can save and then immediately verify the new native filter
against the packaged dashboard contract.

For control-tower sources like `commercial_rhythm_control_tower`, the fit model now treats a report as a secondary follow-up lane, not as a replacement for the CRMA story. In practice that means:

- `TABULAR` formats are favored for true owner-accountability follow-up
- `SUMMARY` formats can still win when they keep good row-level context and filters
- `MATRIX` formats are penalized as dense diagnostic substitutes rather than clean follow-up lanes

## Design Intent

This CLI is meant to improve builder quality, not just route scripts.

The sequence is:

1. resolve the ask
2. retrieve the best local patterns and exemplar surfaces
3. choose the right surface
4. create a neutral build spec
5. create a first-pass draft
6. critique the draft before any mutation or deployment
7. revise the draft into a stronger second pass before any downstream build work
8. package the revised draft into a build-ready execution handoff
9. hand off the package to the exact executor lane with real commands and a saved artifact

The critique is now grounded in two levels of local standards:

- reusable excellence patterns such as `manager_operating_system`, `owner_accountability_report`, and `cross_suite_control_tower`
- named exemplar surfaces such as `forecast_revenue_motions`, `commercial_rhythm_control_tower`, `bdr_manager`, and `executive_revenue_source_truth`

Current exemplar library includes:

- `executive_revenue_source_truth`
- `forecast_revenue_motions`
- `lead_funnel`
- `commercial_rhythm_control_tower`
- `bdr_manager`
- `bdr_rep_queue`
- `revenue_retention_health`
- `executive_product_mix_industry`

## Current Limits

- This is still a planning, critique, and second-pass revision layer, not a direct asset generator.
- The report and native dashboard lanes are draft-level adapters; this repo remains strongest on CRMA execution.
- `draft` is intentionally baseline-only. `revise` is better. `package` and `handoff` are concrete enough for execution prep, but they still stop short of live mutation.
- The excellence library is intentionally small and should grow with more validated gold-standard surfaces.
- The specialist critics are still rules-plus-retrieval, not free-form generative design intelligence yet.

## CRMA Execution Detail

For CRMA surfaces, `execution_plan` is now a `wave_patch_plan`, not just a generic checklist.

That plan includes:

- `baseline_export`: export/lint prerequisites
- `storyboard_patch`: ordered `patch_operations`
- `storyboard_patch.wave_patch_payload`: the machine-facing mutation payload
- `validation`: final guardrails and rollout checks

The `patch_operations` layer is the mutation-ready part. It tells the executor to:

- normalize the PATCH state first
- scaffold pages with stable `page_name` values
- keep nav link names aligned to page names
- upsert section widgets in order
- respect widget-level columnMap strategy
- re-run patch validation before promotion

The `wave_patch_payload` layer is the executor contract. It packages:

- `navigation_contract`: stable page names / nav destinations
- `page_mutations`: ordered pages, sections, widget contracts, and layout bands
- `handoff_link`: the typed follow-up surface to preserve, including report vs dashboard destination semantics
- `validation_contract`: review gates, design constraints, and required checks

`handoff` now writes `wave_patch_payload.json` for CRMA surfaces so the next Wave executor step can work from a concrete mutation payload instead of reconstructing the page/widget contract from prose.

The preferred next executor-side command is:

```bash
python3 scripts/wave_patch_executor.py bundle \
  --payload output/.../wave_patch_payload.json \
  --baseline output/.../live_export/<dashboard_slug>/dashboard.json \
  --output-dir output/.../wave_patch_bundle \
  --json
```

Once the bundle is clean, the preferred next command is the deploy preview:

```bash
python3 scripts/wave_patch_executor.py deploy \
  --state output/.../wave_patch_bundle/dashboard_state.patch.json \
  --baseline output/.../live_export/<dashboard_slug>/dashboard.json \
  --output-dir output/.../wave_patch_deploy_preview \
  --json
```

That keeps the builder brain CLI-first: package the design, export the live baseline, emit the mutation contract, then compile:

- `normalized_baseline_state.json`
- `wave_patch_worklist.json`
- `wave_patch_bundle.json`
- `wave_patch_set.json`
- `dashboard_state.patch.json`
- `wave_patch_autofill_summary.json`
- `wave_patch_fill_requirements.json`
- `wave_patch_query_review_checklist.json`
- `wave_patch_request.json`

before any live PATCH attempt.

`wave_patch_set.json` is the new executor-focused artifact. It carries patch-ready JSON fragments for:

- page scaffolds
- navigation updates
- layout items
- widget payloads
- step placeholders
- the handoff link contract when the follow-up target is a Wave-supported dashboard/page destination

If the handoff target is a Salesforce report, the executor now emits that as `external_handoff` inside the patch set instead of forcing an invalid Wave link widget into the candidate dashboard state.

`dashboard_state.patch.json` is the assembled dry-run candidate state. It applies the patch set onto the normalized exported baseline so you can lint and inspect the full PATCH body before any live Wave mutation.

`wave_patch_autofill_summary.json` records the safe repo-truth defaults the executor was able to apply before human review. Today that includes:

- step type defaults for known CRMA widget families, typically `saql`
- first-pass SAQL query scaffolds derived from the exported baseline dataset and selector context
- first-pass explicit `columnMap` arrays for widgets that need the full 4-key shape
- known dashboard destination ids when the handoff target carries a concrete CRMA label/id that matches local live context

For generic surfaces, the autofill summary also marks heuristic query scaffolds as review-required. They are much better than raw placeholders, but they still need a human or agent review pass before promotion. For known source-specific templates such as `Commercial_Rhythm_Control_Tower`, the executor can now emit concrete query bodies without leaving the bundle in warning status.

`wave_patch_fill_requirements.json` is the remaining unresolved-placeholder checklist. It enumerates every placeholder that still must be filled before a live PATCH, including:

- dashboard/page handoff destination identifiers when a live Wave target is still unresolved

`wave_patch_query_review_checklist.json` is the human/agent review artifact for heuristic query scaffolds. It turns the warning-level query generation into an explicit checklist so the final review pass is narrow and auditable instead of implicit.

For report handoffs specifically, the executor now keeps the target typed in the package and runbook, but it does not emit a Wave link widget. The live API accepted only the dashboard/page link patterns already present in exported CRMA assets, and rejected `destinationType: report`.

`wave_patch_request.json` is the final dry-run deploy artifact. It records the inferred live dashboard id, request path, body size, and state path for the exact Wave PATCH that would be sent. After that preview is clean, rerun the same command with `--apply` to execute the live PATCH.

For the current commercial-rhythm example, the executor now auto-fills:

- five step `type` values
- five source-specific query bodies
- four explicit `columnMap` bindings

That now cuts the unresolved placeholder count from fifteen down to zero, and the commercial-rhythm dry-run bundle returns `status: ok` because those five queries are no longer generic heuristics. The external Salesforce report handoff remains preserved in the package without being pushed into the Wave dashboard state.

This keeps the builder brain aligned to the repo rule that CRMA execution should happen through direct Wave/API workflows, not through legacy Python dashboard builders, while native Salesforce reports and dashboards now have concrete authoring bundles instead of vague handoffs.
