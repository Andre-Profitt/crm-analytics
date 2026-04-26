# Sales Director Canonical Shell Workflow

Date: 2026-04-11

## Why This Exists

The director deck is the main product. It needs a native PowerPoint-authored canonical shell before monthly population can be treated as production-safe.

So the production path is:

1. Start from the real SimCorp PowerPoint template.
2. Use PowerPoint Claude once to author a polished canonical director shell.
3. Promote that authored shell into the canonical shell store.
4. Use the canonical shell for monthly director builds.

Generated shells remain scaffolding only.

## Primary Files

- Authoring prompt builder:
  - `scripts/build_sales_director_shell_author_prompt.py`
- Canonical shell authoring runner:
  - `scripts/run_sales_director_canonical_shell_builder.py`
- Monthly director builder:
  - `scripts/run_sales_director_monthly_master_builder.py`
- Structured fact-pack / slot builder:
  - `scripts/build_validated_director_brief.py`
- Shell contract validator:
  - `scripts/validate_sales_director_shell_contract.py`
- Canonical shell store:
  - `output/sales_director_canonical_shells/`

## Production Flow

### 1. Create the canonical shell candidate

Preferred repo-first path:

1. Run the deterministic validated-baseline builder for the target director.
2. Promote that validated baseline into the canonical shell store.
3. Treat PowerPoint-Claude shell authoring as an optional refinement layer, not the only way to create the first canonical asset.

Direct promotion example:

```bash
python3 scripts/run_sales_director_canonical_shell_builder.py \
  --director-name "Sarah Pittroff" \
  --territory "Central Europe" \
  --snapshot-date 2026-04-10 \
  --baseline-deck-path "output/sales_director_monthly_master_builder/2026-04-10/<run>/sarah-pittroff/deterministic_preview/Sales Director Monthly - Sarah Pittroff (Central Europe) Validated Baseline.pptx" \
  --powerpoint-mode skip \
  --promote-on-success
```

### 2. Optional native shell refinement

Run:

```bash
python3 scripts/run_sales_director_canonical_shell_builder.py \
  --director-name "Sarah Pittroff" \
  --territory "Central Europe" \
  --snapshot-date 2026-04-10 \
  --powerpoint-mode build \
  --promote-on-success
```

This creates:

- a working copy of the SimCorp template
- a shell-authoring prompt
- a PowerPoint-Claude authoring run bundle
- promoted canonical shell copies in:
  - `output/sales_director_canonical_shells/Sales Director Monthly Shell - Sarah Pittroff (Central Europe).pptx`
  - `output/sales_director_canonical_shells/2026-04-10/Sales Director Monthly Shell - Sarah Pittroff (Central Europe).pptx`

### 3. Use the canonical shell in monthly builds

The monthly builder should consume the canonical director shell as the production deck source.

## Operating Rules

- Canonical shells are native PowerPoint assets, not generated shell scaffolds.
- A validated baseline may be promoted directly when it already meets the shell contract and visual standard.
- Monthly content runs should populate the canonical shell, not redesign it.
- The shell contract is only valid if `scripts/validate_sales_director_shell_contract.py` passes.
- Structured fill is the primary handoff: JSON slot payload first, markdown fact pack second.

## Promotion Gate

Do not promote a shell unless:

- PowerPoint opens it without repair
- placeholder text is editorial, not system-field language
- slide sequence matches the director shell contract
- SimCorp branding is preserved
