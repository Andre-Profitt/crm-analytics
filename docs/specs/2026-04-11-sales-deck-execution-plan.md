# Sales Deck Execution Plan

Date: 2026-04-11

## Purpose

This is the fixed execution sequence for the sales deck system.

Use this plan to avoid re-deciding priorities every session. The order is not optional:

1. canonical shell assets
2. validated data contracts
3. structured fill payloads
4. controlled PowerPoint population
5. publish gates
6. new data-source expansion

## Product Hierarchy

1. **Director primary deck**
   - one deck per MD-1
   - this is the main product
2. **Global summary deck**
   - one executive summary deck with one operating slide per region
3. **Regional rollup layer**
   - internal rollup utility
   - feeds the global summary deck
   - not a primary deliverable unless explicitly requested

## Current State

### Complete

- corrected workbook and snapshot pipeline
- director structured fact-pack builder
- regional structured fact-pack builder
- global structured fact-pack builder
- director shell contract
- regional shell contract
- global shell contract
- director shell validator
- regional shell validator
- global shell validator
- director monthly runner
- global monthly runner
- regional monthly runner
  - retained as an internal rollup utility, not a primary product

### In Progress

- canonical shell asset creation
  - director canonical shell
  - regional canonical shell
  - global canonical shell

### Not Started

- KYC missing Salesforce source
- Finance churn source
- target/quota coverage source
- broader Salesforce activity source beyond current hygiene proxies

## Phase Plan

### Phase 0: Data Contract Stabilization

Status: `complete`

Deliverables:
- workbook snapshots
- validated fact packs
- structured fill payloads
- shell contract validators

Exit gates:
- contract validators pass
- focused tests pass
- no slide in any shell lacks a declared support level

### Phase 1: Canonical Shell Assets

Status: `in_progress`

Goal:
- produce native PowerPoint-authored canonical shells for:
  - director
  - global
  - region only if a standalone regional deck is explicitly needed later

Rules:
- generated shells are scaffolds only
- canonical shells are the only publish-safe production inputs
- monthly builders must fail loudly when canonical shells are missing unless explicit fallback is allowed

Exit gates:
- PowerPoint opens shell with no repair banner
- shell uses real SimCorp branding and masters
- no raw placeholder or system-field text remains visible
- shell matches its JSON contract slide-for-slide

### Phase 2: Structured Population

Status: `ready`

Goal:
- fill canonical shells only from:
  - `validated-fact-pack.md`
  - `powerpoint-fill-payload.json`
  - `powerpoint-build-prompt.txt`

Rules:
- PowerPoint worker is not allowed to invent structure
- structured fill payload is the primary slot map
- markdown fact pack supplies nuance and message-title wording

Exit gates:
- slot coverage is complete for all `strong` slides
- `qualified` slides carry explicit caveats where needed
- `placeholder` slides remain clearly marked placeholders

### Phase 3: Publish Gates

Status: `ready`

Goal:
- block leadership publishing when metrics, horizons, or unsupported claims drift

Required publish checks:
- ARR vs ACV labels
- omitted not buried in headline pipeline
- Q1 promise baseline remains qualified
- missing commercial approval coverage
- leftover shell guidance not visible
- unsupported churn or KYC claims not presented as facts

Exit gates:
- deck marked `publishable`
- blockers are explicit when not publishable

### Phase 4: Source Expansion

Status: `pending`

Add these explicit sources:
- `Accounts without KYC Approval | Salesforce`
- `Commercial Approval candidates | Salesforce`
- `Commercial Approval approved 2026 | Salesforce`
- Finance-owned churn reporting
- quota / target coverage source
- broader seller activity source if needed

Exit gates:
- new source is extracted into deterministic snapshot fields
- shell contract updated
- tests updated

## Exact Next Build Order

1. **Create canonical director shell**
   - highest priority
   - director deck is the main product
2. **Create canonical global shell**
   - because the global runner and payload already exist
3. **Run structured population into canonical shells**
   - start with one director pilot
   - then one global pilot
4. **Run publish gate**
5. **Only then add new data sources**
6. **Only after that, decide whether a standalone regional shell is still needed**

## Operational Rules

- No new deck family should be added.
- No shell should be treated as production-safe until it is canonical.
- No slide should become factual unless it is backed by the snapshot contract.
- No desktop automation belongs in the critical path until the shell assets are stable.
- No freeform AI deck generation is allowed in production.

## Entry Points

### Director

- builder: `scripts/run_sales_director_monthly_master_builder.py`
- contract: `config/sales_director_monthly_shell.json`
- validator: `scripts/validate_sales_director_shell_contract.py`

### Regional

- builder: `scripts/run_sales_region_monthly_builder.py`
- contract: `config/sales_region_monthly_shell.json`
- validator: `scripts/validate_sales_region_shell_contract.py`
- role: internal rollup utility unless a standalone regional deck is explicitly needed

### Global

- builder: `scripts/run_sales_global_summary_builder.py`
- contract: `config/sales_global_summary_shell.json`
- validator: `scripts/validate_sales_global_summary_shell_contract.py`

## Definition Of Done

The system is only complete when all of these are true:

- canonical director shell exists
- canonical global shell exists
- director and global shell validators pass
- director and global builders emit:
  - validated fact pack
  - structured fill payload
  - build prompt
- publish gates can block bad decks deterministically
- shell population is constrained to the canonical templates

## Session Discipline

At the start of each session:
- check this plan first
- continue the highest-priority incomplete phase
- do not branch into lower-priority work unless a blocker is explicit

At the end of each session:
- update phase status only if an exit gate actually passed
- record the next single build slice
