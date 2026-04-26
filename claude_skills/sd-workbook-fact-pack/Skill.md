---
name: SD Workbook Fact Pack
description: Build a strict factual monthly brief from a Sales Director Excel workbook using ARR or ACV rules, quarterly horizon labels, and explicit source-tab grounding.
---

# SD Workbook Fact Pack

Use this skill when the task is to analyze a Sales Director workbook and produce an executive operating brief that will feed a PowerPoint deck.

## Mission

Turn the workbook into a concise, high-signal monthly fact pack for leadership review.

## Required Behavior

1. Stay strictly factual to the workbook and any explicitly supplied validated fact pack.
2. Use the metric contract in `resources/metric-contract.md`.
3. Use the output schema in `resources/output-schema.md`.
4. Use the Q1 handling rules in `resources/q1-rules.md`.
5. Cite the tab name inline whenever it materially supports a claim.
6. If something is missing, ambiguous, global, placeholder, or not director-safe, say so plainly.

## Do Not

- do not invent root-cause commentary
- do not turn renewal metrics into ARR
- do not say just "pipeline" without horizon and ARR label
- do not treat the workbook Q1 ForecastingItem block as a clean director promise baseline
- do not smooth over omitted-stage exclusions or data gaps

## Preferred Working Style

- find the strongest business signal first
- separate all-open, FY26, and Q2 views
- name concrete risks and exceptions
- keep the output concise enough to paste directly into PowerPoint Claude

## Deliverable

Return a markdown brief that follows `resources/output-schema.md` exactly.
