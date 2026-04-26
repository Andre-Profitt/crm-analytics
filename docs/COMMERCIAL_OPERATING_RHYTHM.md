# Commercial Operating Rhythm

This document turns the Sales Handbook and live org evidence into the shared
operating rhythm for the CRM Analytics suite.

Use it with:

- `/Users/test/crm-analytics/docs/CONSULTANT_GRADE_CRMA_PLAYBOOK.md`
- `/Users/test/crm-analytics/config/sales_process_codification.json`
- `/Users/test/crm-analytics/config/commercial_operating_model.json`
- `/Users/test/crm-analytics/docs/generated/role_structure_profile_2026-03-11/profile.md`

## Core Model

The live org and the handbook agree on the commercial split:

- `Sales` owns `Land` / new-logo / origination
- `CX` owns most `Expand` and nearly all `Renewal`
- `Services` influences delivery-heavy expansion motion
- `Marketing / BDR` influences demand and account origination

The dashboards must reflect that operating truth.

## Persona Rhythm

### Executive

Cadence:
- weekly to monthly

Primary questions:
- are we on target
- where is confidence weak
- which regions / accounts need intervention

Primary surfaces:
- `Executive Revenue Source Truth`
- future executive retention / product views

### Sales Manager

Cadence:
- weekly

Primary questions:
- which deals need promotion
- which commits need protection
- which omitted deals need cleanup
- which reps need coaching
- which deals require formal review / T&I

Primary surface:
- `Forecast & Revenue Motions`

Primary drill path:
- `Manager -> Owner/Rep -> Opportunity -> Account 360`

### CSM Manager

Cadence:
- weekly to monthly

Primary questions:
- which renewals are at risk
- how much base is protected
- where is churn pressure building
- where is growth on the base happening
- which accounts need save plans / QBR intervention

Primary surface:
- `Revenue Retention & Health`

Primary drill path:
- `Manager -> CSM Owner -> Account -> Account 360`

### Individual

Cadence:
- daily

Primary questions:
- what do I work on next
- which records need escalation
- which account or opportunity needs a plan update today

## Motion Ownership

| Motion | Primary Persona | Secondary Personas | Notes |
|---|---|---|---|
| `Land` | `Sales` | `Marketing`, `BDR` | New-logo and origination motion |
| `Expand` | `CX` | `Sales`, `Services` | Often customer-growth or platform extension motion |
| `Renewal` | `CX` | `Services` | Strict renewal and protected-base motion |
| `Contraction` | `CX` | `Services` | Base erosion / downsell motion |
| `Churn` | `CX` | `Services` | Lost or no-opportunity retained base |

## Dashboard Rules

### Sales Manager dashboards must show

- stage progression
- forecast scenario discipline
- promotion pressure
- commit protection
- omitted cleanup
- rep coaching pressure
- deal review / T&I candidates

### CSM Manager dashboards must show

- strict renewal view
- effective retention / protected-base view
- growth on base
- churn pressure
- save actions
- governance cadence like QBR where possible

### Shared drill target

All manager and executive surfaces should land on:

- `Account 360 & History`

That is where:
- account history
- product movement
- renewal evidence
- expansion evidence
- churn / protection context

should come together.

## Data Contract Rules

Use these keys and signals:

- primary join key: `OwnerId`
- account-owner join key: `Account.OwnerId`
- manager join key: `ManagerId`

Do not rely on:

- `OwnerName` alone
- `ManagerName` alone

Persona classification should combine:

- `UserRole.Name`
- `Title`
- `Division`
- `Department`
- `ManagerId`

## What This Changes

This should become the default rule across the suite:

- `Sales Manager` surfaces are not generic revenue dashboards
- `CSM Manager` surfaces are not generic customer KPI dashboards
- persona and motion ownership should be explicit in dataset design, audit logic, and dashboard navigation
