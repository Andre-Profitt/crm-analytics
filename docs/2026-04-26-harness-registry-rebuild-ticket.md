# Ticket: rebuild `config/harness_registry.json` scope

**Status:** open · **Priority:** P2 · **Owner:** unassigned (operator-driven)
**Trigger:** `tests/test_harness_registry.py::test_validate_registry_json` red on main.
**Filed:** 2026-04-26 · **Related:** PR #7 quarantine triage; commit `ac1c7b2` ("archive 69 dead/experimental scripts").

## What's broken

`config/harness_registry.json` is the canonical inventory of scripts the
project knows about, classified into lanes (intelligence, patch
guardrails, dashboard mutations, etc.) with metadata for command class,
artifact mode, evidence types, and memory tags. The validator
(`scripts/harness_registry.py validate`) currently fails with **84 errors**:

- **40 entries** in the active `scripts` section have a `path` pointing
  at a file that has been moved to `scripts/_archive/` — the file is no
  longer at the registered location AND its lane membership is now
  semantically wrong.
- **21 entries** in `excluded_scripts` point at the same kind of
  archived path. Less severe (the section is meant for "tracked but
  not in active surface"), but still wrong about _where_ the file lives.
- **82 scripts** physically present under `scripts/` are not classified
  in either section (`unclassified_script` errors). The active code
  surface has been growing without registry updates.

## Why "quick-trim it" is the wrong fix

Tempting solution: drop the 40+82 entries to make the validator green.
That makes the registry green by removing its memory of the repo,
which is the same "green but not trustworthy" pattern Track A–D have
been working to eliminate. The registry's job is to tell the truth
about what scripts exist, where they live, and which lane owns them;
discarding entries to silence the validator inverts that.

## What the right fix looks like

A real registry-rebuild pass, operator-driven:

1. **Move archived active entries to `excluded_scripts` (or delete entirely).**
   For each of the 40 entries currently in `scripts` that point at
   `scripts/_archive/`:
   - If the script is genuinely abandoned and won't be restored, _delete_
     the registry entry (the source code is preserved in `_archive/` for
     forensics, but the registry doesn't need to track it).
   - If the script is intentionally archived but might be restored,
     move the entry to `excluded_scripts` with the corrected
     `scripts/_archive/<name>.py` path AND a `reason: "archived <date>"`
     annotation.
   - The validator can be tightened to enforce that `excluded_scripts`
     entries explicitly carry a `reason`.
2. **Classify the 82 unclassified scripts.**
   Walk `scripts/*.py`, decide for each:
   - **Active**: belongs in `scripts` with a real lane assignment,
     command class, and metadata.
   - **Excluded but tracked**: belongs in `excluded_scripts` with a
     reason ("temporary scaffolding for X", "one-shot migration", etc.).
   - **Should be archived**: move the file to `scripts/_archive/`
     instead of the registry chasing it.
3. **Tighten the validator** to:
   - Fail only on active-lane governance breaches (missing required
     metadata on an active entry, wrong command class, etc.).
   - Emit `unclassified_script` and archive-path mismatches as
     `report` / `warning` rather than `error` until the rebuild is
     complete, so the validator stays useful as a guide rail rather
     than a blocker during the cleanup pass.

## Out of scope

- This ticket is **not** about the test infrastructure. The test that
  surfaced this (`test_validate_registry_json`) is the quarantine's
  intended behavior — it should fail when the registry is wrong, and
  the right response is to fix the registry, not the test.
- Track A–D and Track H were intentionally allowed to land while this
  registry drift sat unresolved because they don't depend on
  `harness_registry.json`. Track I (Pandera + JSON Schema dataframe
  contracts) also doesn't depend on the registry.

## Acceptance criteria

- `python3 scripts/harness_registry.py validate` exits 0.
- Every entry in `scripts` points at a file under `scripts/` (not
  `scripts/_archive/`) with a real lane and full metadata.
- Every entry in `excluded_scripts` carries a `reason` field.
- Every `*.py` under `scripts/` is either registered, in
  `excluded_scripts` with a reason, or moved to `scripts/_archive/`.
- One commit per "category" (e.g. one commit moves audit-\* archived
  entries, another classifies the new active scripts) so the diff is
  reviewable rather than a single 100+-entry blob.

## Why this can wait until after Track I

The registry's failures are about _which scripts exist and what they
are for_, not about pipeline correctness. The source-backed monthly
review platform runs against a small explicit subset of scripts
(extract, audit, calibrate, build_warehouse, etc.) that ARE already
registered correctly. The registry rebuild pays off when an operator
needs to grep for "all scripts in lane X" or "all mutating scripts" —
useful, but not on the critical path for v20d evidence or Track I
schema work.
