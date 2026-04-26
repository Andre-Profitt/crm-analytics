# Context Hierarchy System

This repo should keep context in layers, not in one long conversational thread.

The goal is:
- keep the active build cycle small
- preserve the important research and audit evidence
- stop dragging stale browser/runtime/history context into the wrong dashboard

## The Hierarchy

### 1. Program

The broad operating system.

Required fields:
- purpose
- business scope
- canonical role/process model
- domain list

Example:
- `Commercial / GTM Operating System`

### 2. Domain

A major workstream inside the program.

Required fields:
- audience family
- operating cadence
- key business questions
- connected surface sets
- core semantic risks

Examples:
- `BDR`
- `Sales Manager`
- `CSM Manager`
- `Account 360`
- `Product / GTM`

### 3. Connected Surface Set

A group of dashboards that should work together.

Required fields:
- surface-set purpose
- dashboards in the set
- shared drill target
- shared metrics
- shared publish gates

Example:
- `BDR Operating System`
  - `BDR Manager`
  - `BDR Rep Queue`
  - `BDR Campaign / Target List Control`

### 4. Individual Dashboard Brief

The build contract for one dashboard.

Required fields:
- audience
- decisions supported
- metric definitions
- key filters
- drill targets
- action layer
- publish gates

Only one dashboard should be "hot" in active context at a time.

### 5. Active Build Cycle

The only context that should stay "hot" while building.

Required fields:
- active dashboard
- live link
- latest audit
- current target state
- current defects
- next 1-3 upgrades

This must stay small. If it is not helping the current dashboard move forward, it should leave the hot context.

### 6. Evidence Pack

Research, profiles, audits, exports, and notes that should be preserved but not carried around constantly.

Required fields:
- source file
- domain
- why it matters
- whether it is active, supporting, or archival

## Hot Vs Preserved Context

### Keep Hot

- current active dashboard brief
- current audit
- current live export
- current semantic model
- current target improvements

### Preserve But Do Not Keep Hot

- older dashboard iterations that were superseded
- browser/MFA/session problems
- broad cross-domain planning while focused on one dashboard
- fixed runtime bugs with no remaining design impact

## Working Rules

1. Start from the `Program -> Domain -> Connected Surface Set -> Dashboard` chain.
2. Pull only the evidence needed for the current dashboard.
3. Update the active context after each meaningful cycle.
4. Do not replace the evidence pack when compacting; index it.
5. Do not reopen other domains unless the active dashboard depends on them.

## Required Repo Artifacts

- `docs/context/ACTIVE_CONTEXT.md`
- `docs/context/EVIDENCE_INDEX.md`
- `config/context_registry.json`

Optional but recommended:
- dashboard brief templates
- domain-specific active context files
- current-cycle audit links
