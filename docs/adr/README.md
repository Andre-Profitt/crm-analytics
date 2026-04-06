# Architecture Decision Records

This directory holds Architecture Decision Records (ADRs) for the CRM Analytics repo. ADRs capture significant architectural choices, the reasoning behind them, the alternatives considered, and the conditions under which they should be revisited.

## Index

| ADR                                               | Title                                                                               | Status   | Date       |
| ------------------------------------------------- | ----------------------------------------------------------------------------------- | -------- | ---------- |
| [ADR-0001](ADR-0001-kpi-reports-data-backbone.md) | KPI Reports Data Backbone — CRM Analytics Primary, Salesforce Reports as Link Layer | Accepted | 2026-04-06 |

## Conventions

- **Filename:** `ADR-NNNN-kebab-case-title.md`
- **Numbering:** zero-padded, sequential, never reused
- **Status values:** `Proposed`, `Accepted`, `Deprecated`, `Superseded by ADR-NNNN`
- **Supersession:** when an ADR is replaced, mark the old one `Superseded by ADR-NNNN` and link the successor in its References section
- **Scope:** an ADR captures a _decision_, not a how-to. If you find yourself writing implementation steps, that belongs in a spec or plan under `docs/superpowers/`, not here.

## When to write an ADR

Write an ADR when:

- A choice will be hard to reverse later (data backbone, auth model, deploy pipeline)
- Multiple reasonable alternatives exist and the team needs to know why one was chosen
- A constraint (technical, organizational, contractual) shapes the design in a way that is not obvious from the code
- Future maintainers will benefit from knowing the reasoning, not just the outcome
- A decision is implicit in the implementation but undocumented (formalize it)

Don't write an ADR for:

- Routine implementation choices that any reasonable engineer would make the same way
- Decisions that are fully captured in a spec, plan, or in the code itself
- Bug fixes or one-off changes
