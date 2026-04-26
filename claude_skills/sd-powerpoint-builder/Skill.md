---
name: SD PowerPoint Builder
description: Build or rewrite a SimCorp executive Sales Director presentation in PowerPoint from a validated fact pack, preserving template branding and executive slide discipline.
---

# SD PowerPoint Builder

Use this skill when the task is to create or materially rewrite a Sales Director monthly executive deck in PowerPoint.

## Mission

Build an executive enterprise presentation that is:

- SimCorp-branded
- strict on factual reporting
- grounded in a validated fact pack
- explicit about ARR, ACV, and time horizon

## Required Inputs

- the current PowerPoint deck or SimCorp template
- a validated fact pack
- optional review notes from Codex or Excel Claude

## Required Behavior

1. Use the slide blueprint in `resources/slide-blueprint.md`.
2. Use the writing rules in `resources/writing-rules.md`.
3. Preserve the current template’s slide master, layouts, fonts, colors, and overall brand language.
4. Prefer rewriting existing slides over inventing new style patterns when a template deck is already open.
5. Keep every metric explicitly labeled with unit and horizon.

## Slide Intent

This is an executive operating review, not a data dump.

Each slide should answer one management question clearly and show only the minimum proof required.

## Do Not

- do not invent missing Finance churn inputs
- do not turn placeholders into confident claims
- do not bury important metric caveats
- do not call renewal value ARR
- do not merge Q2 forecast and all-open pipeline into one unlabeled number

## Deliverable

Edit the presentation so it aligns to the validated fact pack and the slide blueprint in `resources/slide-blueprint.md`.
