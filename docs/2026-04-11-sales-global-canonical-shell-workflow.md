# Sales Global Canonical Shell Workflow

Date: 2026-04-11

## Why This Exists

The global summary deck is the executive rollup. It needs a native PowerPoint-authored canonical shell before monthly population can be treated as production-safe.

So the production path is:

1. Start from the real SimCorp PowerPoint template.
2. Use PowerPoint Claude once to author a polished canonical global summary shell.
3. Promote that authored shell into the canonical shell store.
4. Use the canonical shell for monthly global builds.

## Primary Files

- Authoring prompt builder:
  - `scripts/build_sales_global_summary_shell_author_prompt.py`
- Canonical shell authoring runner:
  - `scripts/run_sales_global_canonical_shell_builder.py`
- Monthly global builder:
  - `scripts/run_sales_global_summary_builder.py`
- Structured fact-pack / slot builder:
  - `scripts/build_validated_sales_global_summary_brief.py`
- Shell contract validator:
  - `scripts/validate_sales_global_summary_shell_contract.py`
- Canonical shell store:
  - `output/sales_global_canonical_shells/`

## Production Flow

### 1. Author the canonical shell

Run:

```bash
python3 scripts/run_sales_global_canonical_shell_builder.py \
  --snapshot-date 2026-04-10 \
  --powerpoint-mode build \
  --promote-on-success
```

This creates:

- a working copy of the SimCorp template
- a shell-authoring prompt
- a PowerPoint-Claude authoring run bundle
- promoted canonical shell copies in:
  - `output/sales_global_canonical_shells/Sales Global Summary Shell.pptx`
  - `output/sales_global_canonical_shells/2026-04-10/Sales Global Summary Shell.pptx`

### 2. Use the canonical shell in monthly builds

The monthly builder should consume the canonical global shell as the production deck source.

## Operating Rules

- Canonical shells are native PowerPoint assets, not generated shell scaffolds.
- Monthly content runs should populate the canonical shell, not redesign it.
- The shell contract is only valid if `scripts/validate_sales_global_summary_shell_contract.py` passes.
- Structured fill is the primary handoff: JSON slot payload first, markdown fact pack second.

## Promotion Gate

Do not promote a shell unless:

- PowerPoint opens it without repair
- placeholder text is editorial, not system-field language
- slide sequence matches the global shell contract
- SimCorp branding is preserved
