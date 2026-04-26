# Sales Director Modular Monthly Platform Spec

Date: 2026-04-22
Status: Draft

## Purpose

Design the Sales Director monthly reporting system as a modular platform instead of a collection of tightly coupled scripts.

The platform must:

- resolve the correct reporting month automatically
- run on the 1st of every month through one control-plane command
- keep business semantics in one place
- let teams add, remove, or swap reports and data sources without destabilizing the rest of the system
- produce publish-safe artifacts only when gates are green

This spec is intentionally biased toward the current repo and current working lanes. It is not a greenfield rewrite.

## Product Goal

Turn the current monthly workflow into:

```text
period resolver
  -> source adapters
  -> normalized snapshot
  -> metric modules
  -> report registry
  -> structured payloads
  -> deterministic renderers
  -> publish gates
  -> release packet
```

The system should support three report families through the same architecture:

1. Sales Director monthly
2. Sales Global summary
3. Sales Region rollup

## Current State

### Strong

- dated workbook dumps
- normalized director workbook snapshots
- validated fact-pack builder
- structured fill payloads
- deterministic preview deck generation
- preview audit and layout audit
- canonical promotion path
- release packet builder
- monthly cadence wrapper
- tie-out gate

### Weak

- period defaults are hardcoded in key runners
- month and quarter logic is duplicated across scripts
- report registration is implicit in code, not explicit in config
- some legacy lanes still own semantics they should not own
- no single monthly command runs batch -> tie-out -> promotion -> release packet
- no scheduler or lock model exists in the repo

### Architectural Smell

The repo currently behaves as if each deck family is its own pipeline. That is why month titles, quarter rules, and metric definitions can drift.

The correct design is:

- shared platform primitives
- report-family-specific registry entries
- report-family-specific renderers

## Design Principles

- no business period math inside renderers
- no duplicate semantic definitions across extract, snapshot, tie-out, and deck builders
- no report should scrape workbook tabs directly once a normalized snapshot exists
- no report should become publish-safe without passing explicit gates
- optional data must degrade via support levels, not failures
- every metric must have one semantic owner
- every report must be registered, not hardcoded

## Proposed Package Layout

Use a new package under `scripts/` to minimize repo disruption:

```text
scripts/monthly_platform/
  __init__.py
  period.py
  policy.py
  schema.py
  lineage.py
  orchestration.py
  publish.py
  registry/
    sales_director_monthly.json
    sales_global_summary.json
    sales_region_rollup.json
  metrics/
    pipeline.py
    approvals.py
    renewals.py
    q1_review.py
    hygiene.py
    risk.py
  adapters/
    director_extract_cache.py
    workbook_snapshot.py
    external_overlay.py
  payloads/
    sales_director_monthly.py
    sales_global_summary.py
    sales_region_rollup.py
```

The current top-level scripts remain as thin entrypoints and compatibility wrappers during migration.

## Core Modules

### 1. `PeriodContext`

File:

- `scripts/monthly_platform/period.py`

Responsibility:

- resolve all month, quarter, and fiscal labels from one authoritative function

Inputs:

- `as_of_date`
- optional explicit override for `snapshot_date`
- optional calendar policy

Output contract:

```python
PeriodContext(
    reporting_month="2026-04",
    snapshot_date="2026-04-30",
    deck_date="2026-04-30",
    month_title="April 2026",
    current_quarter="Q2 2026",
    prior_quarter="Q1 2026",
    forward_quarter="Q3 2026",
    fiscal_year="FY26",
    reporting_window_start="2026-01-01",
    reporting_window_end="2026-09-30",
    q1_start="2026-01-01",
    q1_end="2026-03-31",
    q2_start="2026-04-01",
    q2_end="2026-06-30",
    q3_start="2026-07-01",
    q3_end="2026-09-30",
)
```

Rules:

- no other module may compute month titles or quarter windows independently
- every runner must accept `PeriodContext`

### 2. `SemanticPolicy`

File:

- `scripts/monthly_platform/policy.py`

Responsibility:

- hold all shared business rules used across the platform

Owned rules:

- active pipeline scope
- omitted handling
- pending approval mapping
- approved 2026 rule
- missing stage 3+ rule
- ARR vs ACV separation
- renewals scope
- Q1 promise qualification

Rules:

- tie-out imports this
- snapshot normalization imports this
- payload builders import this
- legacy adapters import this while they still exist

### 3. `SnapshotSchema`

File:

- `scripts/monthly_platform/schema.py`

Responsibility:

- validate normalized snapshots and payloads through versioned schemas

Director snapshot minimum shape:

```json
{
  "schema_version": "sd-monthly-snapshot/v1",
  "director_name": "Jesper Tyrer",
  "territory": "APAC",
  "snapshot_date": "2026-04-30",
  "period_context": {},
  "sources": [],
  "scorecard": {},
  "pipeline_detail": {},
  "q1_review": {},
  "commercial_approval": {},
  "renewals": {},
  "risk_register": {},
  "data_quality": {},
  "factual_bullets": []
}
```

Rules:

- schema version is required
- additive fields are allowed
- breaking field changes require a new schema version

### 4. `SourceAdapter`

Files:

- `scripts/monthly_platform/adapters/director_extract_cache.py`
- `scripts/monthly_platform/adapters/workbook_snapshot.py`
- `scripts/monthly_platform/adapters/external_overlay.py`

Responsibility:

- isolate raw source loading from metric logic

Examples:

- Salesforce extract cache adapter
- workbook snapshot adapter
- finance overlay adapter
- slipped commentary overlay adapter

Rules:

- adapters emit lineage-rich data
- adapters do not compute slide language
- adapters do not own render logic

### 5. `MetricModule`

Files:

- `scripts/monthly_platform/metrics/*.py`

Responsibility:

- compute one business concept from `snapshot + period_context + policy`

Examples:

- `pipeline.py`
- `approvals.py`
- `renewals.py`
- `q1_review.py`
- `hygiene.py`
- `risk.py`

Rules:

- one module per domain
- no module should reach into PowerPoint or workbook presentation assumptions
- every module must have fixture-backed tests

### 6. `ReportRegistry`

Files:

- `scripts/monthly_platform/registry/*.json`

Responsibility:

- declare which report exists and how it is built

Registry shape:

```json
{
  "report_id": "sales_director_monthly",
  "report_family": "director",
  "enabled": true,
  "snapshot_schema": "sd-monthly-snapshot/v1",
  "payload_builder": "sales_director_monthly",
  "renderer": "build_sales_director_monthly_shell.py",
  "required_metric_modules": [
    "pipeline",
    "approvals",
    "renewals",
    "q1_review",
    "hygiene",
    "risk"
  ],
  "required_gates": [
    "schema",
    "preview_audit",
    "layout_audit",
    "tie_out",
    "release_packet"
  ],
  "optional_sources": [
    "finance_churn",
    "slipped_commentary"
  ]
}
```

This is the mechanism for surgical lift-and-shift.

If a report is removed:

- disable one registry entry

If a report is added:

- add one registry entry plus its tests

### 7. `PayloadBuilder`

Files:

- `scripts/monthly_platform/payloads/*.py`

Responsibility:

- translate normalized metrics into report-family-specific payloads

Rules:

- payload builders consume typed metrics
- payload builders do not re-interpret raw workbook tabs
- payload builders map support levels explicitly

### 8. `MonthlyOrchestrator`

Files:

- `scripts/monthly_platform/orchestration.py`
- thin wrapper in `scripts/run_sales_director_monthly_cadence.py`

Responsibility:

- run the full monthly control plane for one or more report families

Required order:

1. resolve `PeriodContext`
2. load or refresh source artifacts
3. refresh normalized snapshots
4. build canonical monthly outputs
5. run tie-out
6. promote clean outputs
7. build release packet
8. write final monthly manifest

### 9. `PublishGate`

Files:

- `scripts/monthly_platform/publish.py`
- existing release-packet builders and audit scripts remain as dependencies

Responsibility:

- aggregate gate outcomes into one technical verdict and one publish verdict

Verdicts:

- `technical_green`
- `publish_ready`
- `publish_blocked_manual`
- `publish_blocked_system`

## Report Boundary Rules

### Sales Director Monthly

- primary product
- canonical lane required
- tie-out required
- promotion required before publish-safe alias update

### Sales Global Summary

- executive summary product
- depends on regional rollup data
- release packet required

### Sales Region Rollup

- internal utility by default
- supports the global summary
- not treated as a primary deliverable unless explicitly enabled

## Monthly Run Model

Target command:

```bash
python3 scripts/run_sales_director_monthly_cadence.py monthly-run --as-of-date 2026-05-01
```

Expected behavior:

1. resolves `reporting_month=2026-04`
2. resolves `snapshot_date=2026-04-30`
3. runs the canonical monthly batch
4. runs tie-out
5. promotes only clean outputs
6. builds release packet
7. exits non-zero if blocked or failed

## Operational Modes

### `plan`

- resolve the run
- no mutation beyond manifest generation

### `pilot`

- one director or one report-family pilot

### `batch`

- full report-family execution

### `monthly-run`

- scheduled production wrapper
- must be idempotent for a given reporting month

## Scheduling Contract

The repo currently has no scheduler artifact.

Required additions:

- `launchd` job or equivalent scheduler config
- lock file / single-flight guard
- retry policy for transient failures
- latest run aliases
- notification hook

Scheduler policy:

- timezone pinned to `America/New_York`
- first scheduled attempt on day 1 of each month
- rerun safe for same reporting month

## Epic Plan

### Epic 0: Architecture Freeze

Owner:

- PM / tech lead

Participants:

- platform engineer
- analytics engineer
- RevOps analyst

Deliverables:

- approved module boundaries
- approved semantic ownership map
- approved migration sequence

Acceptance:

- this spec is reviewed and frozen
- one owner is assigned for each module family

Estimate:

- 3 days

### Epic 1: Period Resolver And Shared Policy

Owner:

- analytics engineer

Support:

- RevOps analyst
- QA analyst

Tickets:

1. Build `PeriodContext` module.
2. Replace hardcoded monthly defaults in cadence and master-builder runners.
3. Build shared `SemanticPolicy` module.
4. Refactor tie-out to consume shared policy.
5. Add date-boundary tests for month-end and quarter-end transitions.

Acceptance:

- no hardcoded `2026-04-10` defaults remain in monthly runners
- no hardcoded month titles remain in core monthly execution logic
- period tests cover first-of-month and quarter-roll transitions

Estimate:

- 1.5 weeks

### Epic 2: Snapshot Schema And Contracts

Owner:

- analytics engineer

Support:

- QA analyst

Tickets:

1. Introduce director snapshot schema versioning.
2. Validate snapshot writes in `extract_director_workbook_snapshot.py`.
3. Add payload schema validation for structured fill payloads.
4. Add lineage contract assertions.
5. Build three golden fixtures:
   - omitted-heavy
   - approval-heavy
   - normal clean book

Acceptance:

- snapshot and payload writes fail loudly on schema violations
- golden fixtures are committed and exercised in tests

Estimate:

- 1 week

### Epic 3: Metric Module Decomposition

Owner:

- analytics engineer

Support:

- RevOps analyst
- QA analyst

Tickets:

1. Extract `pipeline` metric module.
2. Extract `approvals` metric module.
3. Extract `renewals` metric module.
4. Extract `q1_review` metric module.
5. Extract `hygiene` and `risk` modules.
6. Wire the validated bridge to consume metric modules instead of ad hoc field traversal.

Acceptance:

- each metric module has fixture-backed tests
- no duplicate semantic definitions remain in bridge and tie-out for these domains

Estimate:

- 2 weeks

### Epic 4: Report Registry And Payload Builders

Owner:

- platform engineer

Support:

- slides engineer
- analytics engineer

Tickets:

1. Introduce registry loader.
2. Create registry entry for `sales_director_monthly`.
3. Create registry entry for `sales_global_summary`.
4. Create registry entry for `sales_region_rollup`.
5. Refactor payload builders to be registry-driven.
6. Add registry validation tests.

Acceptance:

- enabling or disabling a report family is config-driven
- adding a report family no longer requires changing orchestrator branching logic

Estimate:

- 1.5 weeks

### Epic 5: Monthly Control Plane

Owner:

- platform engineer

Support:

- analytics engineer
- QA analyst

Tickets:

1. Add `monthly-run` command to the cadence wrapper.
2. Chain plan -> batch -> tie-out -> promotion -> release packet.
3. Add unified monthly manifest.
4. Add director-isolation and fail-fast controls.
5. Add resumable rerun behavior.

Acceptance:

- one command can run a full reporting month end to end
- one failed director does not corrupt the batch manifest

Estimate:

- 1.5 weeks

### Epic 6: Promotion And Publish Packet Hardening

Owner:

- slides engineer

Support:

- QA analyst
- PM / product owner

Tickets:

1. Make tie-out a required blocker in release packet.
2. Gate canonical promotion on clean audits and release packet readiness.
3. Add explicit manual overlay blockers to publish packet.
4. Add stable `latest.json` and `latest.md` monthly aliases for downstream ops.
5. Add human-readable publish verdict summary.

Acceptance:

- no canonical promotion on red tie-out
- release packet clearly distinguishes technical green from manual-blocked publish status

Estimate:

- 1 week

### Epic 7: Scheduler, Reliability, And Notifications

Owner:

- platform engineer

Support:

- IT / admin
- QA analyst

Tickets:

1. Add scheduler artifact.
2. Add lock file and single-flight guard.
3. Add retry policy for auth and render transients.
4. Add notification adapter for success, blocked, and failure states.
5. Add runbook for operators.

Acceptance:

- monthly-run can be scheduled unattended
- concurrent launches are rejected safely
- failures are diagnosable from artifacts and notifications

Estimate:

- 1 week

### Epic 8: Migration And Legacy Retirement

Owner:

- PM / tech lead

Support:

- all lanes

Tickets:

1. Run one historical month through the new control plane.
2. Run one parallel month against the current process.
3. Compare outputs, blocker rate, and operator effort.
4. Freeze legacy lanes to parity-only mode.
5. Mark old paths as deprecated in docs.

Acceptance:

- two clean monthly cycles complete
- legacy monthly lane is no longer the default path

Estimate:

- 1 calendar month for parallel validation

## Recommended Staffing

### Engineering

- 1 staff platform engineer
- 1 senior analytics engineer
- 1 slides / presentation engineer

### Analysis And QA

- 1 QA / validation analyst
- 1 RevOps analyst

### Delivery

- 1 PM / technical program lead

## Sequence

The build order is not optional:

1. architecture freeze
2. period resolver and shared policy
3. snapshot schema and fixtures
4. metric modules
5. report registry
6. monthly control plane
7. publish hardening
8. scheduler and ops
9. migration and legacy retirement

## Milestones

### Milestone 1

Title:

- calendar-safe monthly execution foundation

Exit:

- period resolver live
- hardcoded defaults removed
- shared policy introduced

### Milestone 2

Title:

- modular semantic platform

Exit:

- metric modules live
- schema validation live
- registry live for director monthly

### Milestone 3

Title:

- one-command monthly run

Exit:

- monthly-run chains all required stages
- release packet includes tie-out and publish blockers

### Milestone 4

Title:

- unattended monthly operations

Exit:

- scheduler, lock, retries, and notifications live
- parallel-run month completed cleanly

## Success Metrics

- first-of-month run requires no code edits for dates
- monthly tie-out is green by default
- release packet generation is automatic and stable
- report-family additions require registry plus tests, not orchestration rewrites
- legacy scripts stop receiving new semantic logic
- operator review time per month drops materially

## Risks

### Risk 1

- hidden semantics remain inside workbook-driven or legacy scripts

Mitigation:

- move business rules into `SemanticPolicy`

### Risk 2

- manual overlays keep technical-green months from being publish-ready

Mitigation:

- treat overlays as explicit publish blockers, not hidden assumptions

### Risk 3

- architecture effort expands into a rewrite

Mitigation:

- keep current runners as wrappers and migrate incrementally behind them

### Risk 4

- teams add new data sources directly into deck logic again

Mitigation:

- require source adapter + metric module + registry entry + tests for every new source

## Immediate Next Tickets

Start here:

1. implement `PeriodContext`
2. remove hardcoded monthly defaults from cadence and master-builder entrypoints
3. implement `SemanticPolicy`
4. wire tie-out and validated bridge to shared policy
5. add `monthly-run` wrapper command

Those five tickets are the shortest path from the current working repo to a real monthly platform.
