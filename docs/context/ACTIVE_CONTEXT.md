# Active Context

Last updated: 2026-03-13

## Program

- `Commercial / GTM Operating System`

## Domain

- `BDR`

## Connected Surface Set

- `BDR Operating System`
- dashboards:
  - `BDR Manager`
  - `BDR Rep Queue`
  - `BDR Campaign / Target Control`

## Active Dashboard

- `BDR truth-layer redesign`
- no single dashboard is active until the account/contact truth layer is defined

## Current Build Cycle

Purpose:
- turn the BDR suite into an institutional account-based operating system for North America before further dashboard refinement

Current target state:
- explicit account-based truth layer with 4 grains:
  - account universe
  - contact coverage
  - lead execution
  - opportunity handoff
- stage `2 -> 3` handoff modeled from the sales handbook gate, not generic conversion
- BDR dashboards must reflect prospect accounts, target accounts, former clients, and contact/persona coverage
- daily rep execution remains important, but only as one layer of the system

Current connected surface:
- manager companion:
  - `https://simcorp.lightning.force.com/analytics/dashboard/0FKTb0000000IzROAU`
- rep companion:
  - `https://simcorp.lightning.force.com/analytics/dashboard/0FKTb0000000J13OAE`
- GTM control companion:
  - `https://simcorp.lightning.force.com/analytics/dashboard/0FKTb0000000JU5OAM`

Current must-read files:
- `/Users/test/crm-analytics/docs/generated/BDR_EXECUTION_PLAN_2026-03-13.md`
- `/Users/test/crm-analytics/docs/generated/BDR_ELITE_OPERATING_SYSTEM_PLAN_2026-03-12.md`
- `/Users/test/crm-analytics/docs/generated/BDR_PROCESS_AND_DATA_STORY_2026-03-12.md`
- `/Users/test/crm-analytics/docs/generated/BDR_STAGE_GATE_AND_ACCOUNT_MODEL_2026-03-13.md`
- `/Users/test/crm-analytics/docs/generated/BDR_ACCOUNT_BASED_GTM_THEORY_2026-03-13.md`
- `/Users/test/crm-analytics/docs/generated/bdr_operating_state_2026-03-12/profile.md`
- `/Users/test/crm-analytics/docs/generated/bdr_field_readiness_2026-03-12/profile.md`
- `/Users/test/crm-analytics/docs/generated/bdr_activity_model_2026-03-12/profile.md`
- `/Users/test/crm-analytics/docs/generated/BDR_GTM_CAMPAIGN_PLAYBOOK_2026-03-12.md`
- `/Users/test/crm-analytics/docs/generated/bdr_audit_2026-03-12_rep_queue_refocus/audit.md`
- `/Users/test/crm-analytics/docs/generated/live_asset_exports_review/bdr_rep_queue_refocus/bdr_rep_queue/summary.json`

Current semantic risks:
- dashboards are still too lead-first
- owned prospect accounts and contact coverage are under-modeled
- lead activity is undercounted if only lead-linked tasks/events are used
- contact/account activity must be separated from strict lead SLA
- client/prospect/former-client classification is partial, not universal
- campaign/source attribution is usable but not enterprise-perfect
- sourced ARR should not be a hero metric yet

Current publish gates:
- no broken widgets
- no account-blind BDR views
- no false precision around sourced pipeline/ARR
- BDR suite can answer:
  - which prospect and former-client accounts matter now
  - which personas inside those accounts are covered or uncovered
  - what this rep should do today
  - which leads/responders/meetings/handoffs need action now
  - how this rep is performing on SLA, response, meetings, and handoff
  - which personas/products/segments are working
  - where target-account and re-engagement actions should happen next

## Next Connected Dashboard

- `BDR Operating System rebuild from truth layer`
- purpose:
  - define the missing account/contact truth layer
  - then rework manager, rep, and GTM control on top of it

## Not Hot Right Now

These stay preserved in the evidence pack, but should not dominate working context unless needed:
- revenue manager / CSM manager details
- legacy browser/MFA/session issues
- abandoned automation-loop discussions
- older superseded BDR dashboard iterations
