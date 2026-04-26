---
name: sd-deck-publish-gate
description: Use when deciding whether a Sales Director monthly deck is ready for leadership. Checks slide coverage, factual trust, narrative quality, missing inputs, and publish blockers using the master-builder artifacts.
---

# SD Deck Publish Gate

Use this skill when the question is "is the deck ready?" or "what blocks publish?"

## Start Here

1. Read `references/publish-checklist.md`.
2. Inspect the latest master-builder `manifest.json`.
3. Inspect the validated fact pack and PowerPoint review message for the director.

## Gate Logic

A deck is not publishable if any of these are true:

- the validated fact pack is missing
- key slides are absent or materially weak
- ARR or ACV labeling is wrong
- quarter and horizon labels are ambiguous
- commercial approval gaps are missing
- Q1 promised vs delivered is missing or misleading
- churn placeholders are presented as settled fact

## Expected Outputs

Return:

- blockers
- non-blocking polish items
- exact next action to get to publishable
