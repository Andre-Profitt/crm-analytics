---
name: sd-fact-gate
description: Use when validating Sales Director workbook, Claude, or deck claims in this repo. Covers ARR versus ACV, horizon labeling, omitted-stage treatment, Q1 ambiguity, and validated fact-pack checks.
---

# SD Fact Gate

Use this skill whenever a number or claim needs to be trusted before it reaches a deck.

## Start Here

1. Read `references/fact-check-order.md`.
2. Prefer the snapshot JSON and validated fact pack over raw deck text or raw Claude text.
3. Rebuild the claim from source tabs or cached snapshot fields when in doubt.

## Non-Negotiables

- pipeline metrics must specify ARR and horizon
- renewal amount must be ACV
- omitted-stage amounts must not be silently merged into active headline pipeline
- workbook Q1 forecast blocks may be global reference only
- mixed-currency or unlabeled numbers are not publishable

## Primary Files

- `output/director_workbook_snapshots/<date>/<director>.json`
- `output/claude_office_etl/.../validated-fact-pack.md`
- `scripts/build_validated_director_brief.py`
- `scripts/extract_director_workbook_snapshot.py`
