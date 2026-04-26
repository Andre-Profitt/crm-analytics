# Sales Director Modular Platform Delivery Plan

Date: 2026-04-22
Status: Draft
Companion spec:

- [2026-04-22-sales-director-modular-monthly-platform-spec.md](/Users/test/crm-analytics/docs/specs/2026-04-22-sales-director-modular-monthly-platform-spec.md)

## Objective

Deliver a modular monthly reporting platform for Sales Director, Global Summary, and Regional Rollup that:

- runs on the 1st of every month
- resolves reporting dates automatically
- keeps metric semantics centralized
- supports clean report lift-and-shift
- blocks unsafe promotion
- emits a release packet and publish verdict

This plan assumes incremental migration from the current working repo, not a rewrite.

## Delivery Model

Cadence:

- 2-week engineering sprints
- 1-week hardening buffer before live monthly cutover
- 1 real parallel-run month before defaulting to the new control plane

Program length:

- 6 engineering sprints
- 1 calendar month for parallel validation

## Team Shape

### Engineering

- `Platform Lead`
  - monthly orchestration
  - scheduling
  - locks
  - manifest model
- `Analytics Engineer`
  - period resolver
  - semantic policy
  - snapshot schema
  - metric modules
- `Slides Engineer`
  - payload builders
  - registry-driven renderers
  - promotion and publish-safe shell handling

### Analysis / Validation

- `RevOps Analyst`
  - metric ownership
  - business rule validation
  - monthly signoff policy
- `QA Analyst`
  - golden fixtures
  - gate matrix
  - regression audit
  - UAT

### Delivery

- `PM / TPM`
  - sequence
  - dependencies
  - sprint planning
  - stakeholder alignment
  - rollout

## Workstream Ownership

| Workstream | Primary owner | Secondary owner |
|---|---|---|
| Period and calendar model | Analytics Engineer | RevOps Analyst |
| Shared semantic policy | Analytics Engineer | QA Analyst |
| Snapshot and payload schemas | Analytics Engineer | QA Analyst |
| Report registry | Platform Lead | Slides Engineer |
| Metric module extraction | Analytics Engineer | RevOps Analyst |
| Monthly control plane | Platform Lead | Analytics Engineer |
| Canonical promotion / publish gates | Slides Engineer | QA Analyst |
| Scheduler / locks / notifications | Platform Lead | QA Analyst |
| UAT / rollout | PM / TPM | RevOps Analyst |

## Program Epics

### Epic A

Title:

- calendar-safe monthly execution

Outcome:

- no hardcoded monthly defaults remain in monthly runners

### Epic B

Title:

- centralized semantic policy

Outcome:

- one shared definition for omitted handling, approval mapping, ARR/ACV, renewals, and quarter scope

### Epic C

Title:

- versioned snapshot and payload contracts

Outcome:

- new data can be added without breaking existing reports

### Epic D

Title:

- report registry and modular payload builders

Outcome:

- reports can be added or removed surgically

### Epic E

Title:

- one-command monthly control plane

Outcome:

- monthly run executes batch build, tie-out, promotion, and release packet in one path

### Epic F

Title:

- unattended monthly operations

Outcome:

- scheduled monthly run with lock, retry, notification, and publish packet

## Sprint 0

Title:

- program setup and backlog freeze

Duration:

- 3 days

Goal:

- align owners, freeze sequencing, and create an execution-ready backlog

Engineer tickets:

- `PM-001` create epic board and ticket IDs for all workstreams
- `PM-002` freeze module boundary decisions from the platform spec
- `PM-003` define branch and review rules for platform migration

Analyst tickets:

- `AN-001` map each current monthly metric to one owner and one current semantic source
- `AN-002` list all manual overlays and publish blockers

Exit gate:

- kickoff packet exists
- owners are assigned
- ticket backlog is prioritized and dependency-tagged

## Sprint 1

Title:

- period resolver and runner hygiene

Duration:

- 2 weeks

Goal:

- remove date fragility from monthly execution

Engineer tickets:

- `ENG-101` build `scripts/monthly_platform/period.py`
- `ENG-102` add `PeriodContext` tests for:
  - first day of month
  - quarter rollover
  - year rollover
- `ENG-103` remove hardcoded defaults from:
  - [run_sales_director_monthly_cadence.py](/Users/test/crm-analytics/scripts/run_sales_director_monthly_cadence.py:1)
  - [run_sales_director_monthly_master_builder.py](/Users/test/crm-analytics/scripts/run_sales_director_monthly_master_builder.py:1)
- `ENG-104` add `--as-of-date` and resolved `snapshot_date` / `deck_date` support to monthly entrypoints
- `ENG-105` ensure manifests record resolved period context explicitly

Analyst tickets:

- `AN-101` define the official month-resolution policy
  - first-of-month target
  - previous month-end snapshot
  - handling when the 1st falls before data readiness
- `AN-102` validate quarter naming and month title expectations for leadership decks

QA tickets:

- `QA-101` create acceptance cases for May 1, 2026, July 1, 2026, and January 1, 2027

Exit gate:

- monthly runners no longer default to a fixed historical date
- period context is explicit and tested

## Sprint 2

Title:

- shared semantic policy and golden fixtures

Duration:

- 2 weeks

Goal:

- eliminate semantic drift at the policy layer

Engineer tickets:

- `ENG-201` build `scripts/monthly_platform/policy.py`
- `ENG-202` move active-pipeline logic into shared policy
- `ENG-203` move approval status mapping into shared policy
- `ENG-204` move ARR vs ACV rules into shared policy
- `ENG-205` wire tie-out and validated bridge to shared policy

Analyst tickets:

- `AN-201` approve canonical definitions for:
  - active pipeline
  - omitted
  - pending approval
  - approved 2026
  - missing stage 3+
  - renewals ACV
  - Q1 promise qualification
- `AN-202` nominate three golden directors:
  - omitted-heavy
  - approval-heavy
  - normal clean book

QA tickets:

- `QA-201` build fixture set for the three golden directors
- `QA-202` add regression tests for policy edge cases

Exit gate:

- shared policy exists
- tie-out and validated bridge consume it
- golden fixtures are committed and passing

## Sprint 3

Title:

- snapshot and payload contract versioning

Duration:

- 2 weeks

Goal:

- make the platform safe for adding or removing data without breaking consumers

Engineer tickets:

- `ENG-301` add snapshot schema versioning
- `ENG-302` add payload schema versioning
- `ENG-303` validate normalized snapshot writes
- `ENG-304` validate structured payload writes
- `ENG-305` add schema migration compatibility rules for optional fields

Analyst tickets:

- `AN-301` classify existing data elements as:
  - required
  - optional
  - placeholder-capable
- `AN-302` define support-level downgrade rules for missing data

QA tickets:

- `QA-301` add schema regression tests for additive and breaking changes
- `QA-302` add fixture-based compatibility checks

Exit gate:

- snapshot and payload artifacts fail loudly on schema violations
- optional data can be absent without breaking runs

## Sprint 4

Title:

- metric module extraction

Duration:

- 2 weeks

Goal:

- decompose business logic into reusable modules

Engineer tickets:

- `ENG-401` extract `pipeline` metric module
- `ENG-402` extract `approvals` metric module
- `ENG-403` extract `renewals` metric module
- `ENG-404` extract `q1_review` metric module
- `ENG-405` extract `hygiene` metric module
- `ENG-406` extract `risk` metric module
- `ENG-407` refactor director validated bridge to consume metric modules

Analyst tickets:

- `AN-401` verify metric outputs against current trusted monthly artifacts
- `AN-402` approve report-level support mapping for each metric domain

QA tickets:

- `QA-401` add one focused regression test per metric module
- `QA-402` create field-diff checks between current and modular outputs

Exit gate:

- director monthly payload is built from metric modules, not ad hoc field traversal
- metric modules are individually testable

## Sprint 5

Title:

- report registry and lift-and-shift architecture

Duration:

- 2 weeks

Goal:

- make reports modular and configurable

Engineer tickets:

- `ENG-501` add report registry loader
- `ENG-502` create registry entry for `sales_director_monthly`
- `ENG-503` create registry entry for `sales_global_summary`
- `ENG-504` create registry entry for `sales_region_rollup`
- `ENG-505` build payload-builder dispatch by registry
- `ENG-506` add registry validation tests

Analyst tickets:

- `AN-501` define current required and optional sources per report family
- `AN-502` decide which reports are enabled by default vs utility-only

QA tickets:

- `QA-501` test enable / disable behavior per report family
- `QA-502` test missing optional-source behavior

Exit gate:

- report family behavior is registry-driven
- adding or removing a report no longer requires editing orchestration branches directly

## Sprint 6

Title:

- monthly control plane and publish automation

Duration:

- 2 weeks

Goal:

- produce the one-command monthly path

Engineer tickets:

- `ENG-601` add `monthly-run` subcommand to the cadence wrapper
- `ENG-602` chain:
  - period resolution
  - canonical batch build
  - tie-out
  - canonical promotion
  - release packet
- `ENG-603` add unified monthly manifest
- `ENG-604` add explicit exit codes for:
  - ok
  - partial
  - blocked
  - failed
- `ENG-605` add tie-out result into the release packet
- `ENG-606` block promotion when tie-out or audits are red

Analyst tickets:

- `AN-601` approve final publish-blocker taxonomy
- `AN-602` validate monthly release packet content with stakeholders

QA tickets:

- `QA-601` run one full historical month through `monthly-run`
- `QA-602` assert release packet behavior for green and blocked cases

Exit gate:

- one command can execute the monthly control plane end to end
- promotion and release packet respect the gate state deterministically

## Sprint 7

Title:

- scheduler, reliability, and operational hardening

Duration:

- 2 weeks

Goal:

- make the monthly run unattended and recoverable

Engineer tickets:

- `ENG-701` add scheduler artifact for first-of-month execution
- `ENG-702` add lock file / single-flight protection
- `ENG-703` add retry policy for transient auth and render failures
- `ENG-704` add notification adapter for:
  - started
  - succeeded
  - blocked
  - failed
- `ENG-705` add resumability from the monthly manifest

Analyst tickets:

- `AN-701` define operator actions for:
  - blocked by manual overlay
  - blocked by data readiness
  - blocked by publish gate

QA tickets:

- `QA-701` simulate rerun and lock conflict cases
- `QA-702` simulate transient stage failure and retry behavior

Exit gate:

- monthly-run can be scheduled unattended
- concurrency and transient failures are handled safely

## Parallel-Run Month

Title:

- production confidence validation

Duration:

- 1 calendar month

Goal:

- prove the new monthly platform against the real business cycle

Engineer tickets:

- `ENG-801` run new control plane in parallel with existing process
- `ENG-802` capture artifact diffs and blocker deltas

Analyst tickets:

- `AN-801` compare business outputs and operator effort
- `AN-802` log any semantic mismatches or workflow blockers

QA tickets:

- `QA-801` certify tie-out, release packet, and promotion behavior across the month

PM tickets:

- `PM-801` run go-live readiness review
- `PM-802` approve legacy lane freeze criteria

Exit gate:

- one full monthly cycle completes with no critical surprises

## Go-Live And Legacy Freeze

Goal:

- make the modular monthly platform the default path

Tasks:

- switch operator documentation to `monthly-run`
- mark legacy lanes as parity-only
- reject new semantic logic in legacy deck scripts unless explicitly approved as temporary adapters
- update runbooks and onboarding docs

Success criteria:

- analysts use release packets and monthly manifests instead of ad hoc script chains
- monthly jobs no longer require manual date edits

## Analyst Work Queue

These are not engineering sidecars. They are required for completion.

1. metric ownership matrix
2. month-resolution business rule
3. support-level downgrade policy
4. report-family required vs optional source map
5. publish-blocker taxonomy
6. golden-director fixture approval
7. parallel-run signoff log

## PM Work Queue

1. maintain dependency graph and critical path
2. hold weekly architecture review until Sprint 4 exit
3. keep scope from expanding into a rewrite
4. maintain blocker log for:
   - manual overlays
   - scheduler dependencies
   - missing source systems
5. run go-live review after the parallel month

## Critical Path

The critical path is:

1. `PeriodContext`
2. `SemanticPolicy`
3. schema versioning
4. metric module extraction
5. report registry
6. `monthly-run`
7. release packet + promotion hard gate
8. scheduler and lock model

If any of these slip, first-of-month unattended operation slips with them.

## Non-Critical But Important

- PowerPoint review refinement
- broader source expansion
- new report-family additions beyond current three
- richer notifications

These should not block the core monthly platform unless they affect publish safety directly.

## Risks

### Risk

- hidden business logic remains in legacy scripts

Mitigation:

- block new semantic changes outside the shared policy and metric modules

### Risk

- the team keeps using ad hoc runs because the new path feels “too strict”

Mitigation:

- make the release packet and monthly manifest easier to consume than the raw scripts

### Risk

- month-end data readiness is later than the 1st

Mitigation:

- build the scheduler around explicit period resolution and rerun-safe idempotence

### Risk

- report modularity creates inconsistent slide quality

Mitigation:

- keep shell contracts and preview audits mandatory

## Metrics For Program Success

- no manual date edits required for monthly runs
- monthly run exit state is understandable from one manifest
- tie-out is green by default
- report add/remove work is registry-driven
- release packet is the default signoff artifact
- operator hours per month decline materially

## First 10 Tickets To Start Now

1. `ENG-101` build `PeriodContext`
2. `AN-101` define official month-resolution policy
3. `ENG-103` remove hardcoded monthly defaults
4. `QA-101` add date-boundary tests
5. `ENG-201` build shared `SemanticPolicy`
6. `AN-201` approve policy definitions
7. `QA-201` commit golden fixture set
8. `ENG-301` add snapshot schema versioning
9. `ENG-601` create `monthly-run` command
10. `ENG-605` add tie-out into the release packet

Those 10 tickets establish the control plane, the semantic contract, and the first publish-safe automation boundary.
