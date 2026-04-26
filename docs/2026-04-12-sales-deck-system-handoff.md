# Sales Deck System Handoff (2026-04-12)

This is the current operational handoff for the Sales Director / Global Summary deck system.

The system is now stable on the repo-side baseline path:

`validated fact pack -> structured fill payload -> deterministic baseline deck -> canonical shell -> release packet`

Desktop PowerPoint / Excel automation is not in the critical path for the current production state.

## Current State

### Production status

- Director lane: working
- Global lane: working
- Canonical shell promotion: working
- Publish packet: working
- External source status packet: working

### Latest release decision

- Snapshot date: `2026-04-10`
- Publish ready: `true`
- Blocker count: `0`

Primary handoff artifacts:

- Release packet markdown: `output/sales_deck_release_packets/latest.md`
- Release packet JSON: `output/sales_deck_release_packets/latest.json`
- External source status markdown: `output/sales_deck_external_source_packets/latest.md`
- External source status JSON: `output/sales_deck_external_source_packets/latest.json`

## Product Shape

### Director primary deck

One deck per MD-1:

- Megan Miceli
- Patrick Gaughan
- Jesper Tyrer
- Sarah Pittroff
- Francois Thaury
- Dan Peppett
- Christian Ebbesen
- Mourad Essofi
- Adam Steinhaus

### Global summary deck

One executive summary deck with:

- APAC slide
- EMEA slide
- North America slide
- global commercial approval view
- appendix / guardrails

### Regional rollup

Regional logic is an internal support layer for the global summary. It is not the primary user-facing deliverable.

## Canonical Assets

### Director canonical shells

Root:

- `output/sales_director_canonical_shells`

Current production batch source:

- `output/sales_director_monthly_master_builder/2026-04-10/20260412-201252`

Promotion summary:

- `output/sales_director_monthly_master_builder/2026-04-10/20260412-201252/canonical-promotion-summary.json`

### Global canonical shell

Root:

- `output/sales_global_canonical_shells`

Current production source:

- `output/sales_global_summary_builder/2026-04-10/20260412-201240`

Promotion manifest:

- `output/sales_global_canonical_shell_builder/2026-04-10/20260412-201407/manifest.json`

## Stable Entry Points

### Director monthly builder

- `scripts/run_sales_director_monthly_master_builder.py`

This now resolves canonical shells by default and emits:

- validated fact pack
- structured fill payload
- deterministic baseline deck
- render montage
- font audit
- layout audit
- text audit

### Global summary builder

- `scripts/run_sales_global_summary_builder.py`

This now follows the same repo-side baseline path and promotes into the canonical global shell lane.

### Release packet builder

- `scripts/build_sales_deck_release_packet.py`

Use:

```bash
python3 scripts/build_sales_deck_release_packet.py --snapshot-date 2026-04-10
```

### External source packet builder

- `scripts/build_sales_deck_external_source_packet.py`

Use:

```bash
python3 scripts/build_sales_deck_external_source_packet.py --snapshot-date 2026-04-10
```

## Governing Contracts

- Architecture: `config/sales_director_ai_workstation_architecture.json`
- Execution plan: `config/sales_deck_execution_plan.json`
- Director shell contract: `config/sales_director_monthly_shell.json`
- Global shell contract: `config/sales_global_summary_shell.json`
- External source contract: `config/sales_deck_external_source_contract.json`
- Source map: `docs/specs/2026-04-11-sales-deck-family-source-map.md`

## Current Publish Packet

The latest release packet already resolves the latest clean system state:

- `output/sales_deck_release_packets/latest.md`
- `output/sales_deck_release_packets/latest.json`

Current packet includes:

- 9-director batch status
- links to deterministic baseline decks
- links to montages
- canonical shell roots
- global summary run
- external source packet summary

## Current External Source Status

The source-gap layer is now explicit instead of implicit.

Current status:

- Provided: `0`
- Pending: `3`
- Proxy covered by workbook: `2`

Tracked sources:

- Finance churn overlay
- KYC Not Completed
- Commercial Approval Candidates by Stage
- Commercial Approval approved 2026
- Salesforce activity detail

Current source packet:

- `output/sales_deck_external_source_packets/latest.md`
- `output/sales_deck_external_source_packets/latest.json`

Finance request scaffolds:

- `output/sales_deck_external_source_packets/2026-04-10/20260412-203858/finance_churn_request.md`
- `output/sales_deck_external_source_packets/2026-04-10/20260412-203858/finance_churn_request.csv`
- `output/sales_deck_external_source_packets/2026-04-10/20260412-203858/finance_churn_request_email.md`

## What Is Done

- Director deck visuals are stabilized enough to run deterministic production baselines.
- Global summary deck is on the same production pattern.
- Canonical director shells exist for all 9 directors.
- Canonical global shell exists.
- Release packet exists and has stable `latest` aliases.
- External source packet exists and has stable `latest` aliases.
- Audits are wired into the baseline path.

## What Is Not Done

- Finance churn overlay is still pending owner intake.
- KYC data is still a pinned Salesforce source, not an ingested artifact.
- Rich Salesforce activity export is still a design gap; current deck uses workbook proxies.
- PowerPoint add-in polish is optional and not part of the production-critical path.

## Recommended Next Work

### Priority 1: ingest one real external source

Best first candidates:

1. `kyc_not_completed_salesforce`
2. `finance_churn_overlay`

Target input root:

- `output/sales_deck_external_inputs/<snapshot-date>/<source-id>/`

### Priority 2: wire the first real source into the validated deck contract

Most likely order:

1. KYC
2. Finance churn
3. richer Salesforce activity

### Priority 3: keep PowerPoint optional

If PowerPoint Claude comes back in, it should be a narrow polish layer on top of canonical shells and validated baseline decks, not the primary rendering lane.

## Operating Rule

Do not regress the system back to:

- generated shells as production assets
- freeform AI deck generation
- desktop automation in the critical path
- untracked external data dependencies

The current system is usable because those failure modes were removed from the production path.
