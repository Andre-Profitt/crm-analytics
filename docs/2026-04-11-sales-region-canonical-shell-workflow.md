# Sales Region Canonical Shell Workflow

Date: 2026-04-11

## Why This Exists

The generated regional shell built via `python-pptx` is not production-safe as the primary PowerPoint source.
PowerPoint repairs that deck on open, and Claude PowerPoint does not reliably bind to repaired decks.

So the production path is now:

1. Start from the real SimCorp PowerPoint template.
2. Use PowerPoint Claude once to author a polished canonical regional shell.
3. Promote that authored shell into the canonical shell store.
4. Use the canonical shell for monthly regional builds.

Generated shells remain a fallback scaffold only.

## Primary Files

- Authoring prompt builder:
  - `scripts/build_sales_region_shell_author_prompt.py`
- Canonical shell authoring runner:
  - `scripts/run_sales_region_canonical_shell_builder.py`
- Monthly regional builder:
  - `scripts/run_sales_region_monthly_builder.py`
- Structured fact-pack / slot builder:
  - `scripts/build_validated_sales_region_brief.py`
- Shell contract validator:
  - `scripts/validate_sales_region_shell_contract.py`
- Canonical shell store:
  - `output/sales_region_canonical_shells/`

## Production Flow

### 1. Author the canonical shell

Run:

```bash
python3 scripts/run_sales_region_canonical_shell_builder.py \
  --region-name EMEA \
  --snapshot-date 2026-04-10 \
  --powerpoint-mode build \
  --promote-on-success
```

This creates:

- a working copy of the SimCorp template
- a shell-authoring prompt
- a PowerPoint-Claude authoring run bundle
- promoted canonical shell copies in:
  - `output/sales_region_canonical_shells/Sales Region Monthly Shell - EMEA.pptx`
  - `output/sales_region_canonical_shells/2026-04-10/Sales Region Monthly Shell - EMEA.pptx`

### 2. Use the canonical shell in monthly builds

Run:

```bash
python3 scripts/run_sales_region_monthly_builder.py \
  --snapshot-date 2026-04-10 \
  --region-name EMEA \
  --powerpoint-mode build
```

This now requires a canonical shell by default. If it is missing, the builder fails loudly.

Each monthly run now produces a deterministic regional bundle:

- validated fact pack: `validated-fact-pack.md`
- structured slot payload: `powerpoint-fill-payload.json`
- PowerPoint population prompt: `powerpoint-build-prompt.txt`
- shell resolution metadata in `manifest.json`

Only use generated shell scaffolding if you are explicitly doing non-publish-safe template work:

```bash
python3 scripts/run_sales_region_monthly_builder.py \
  --snapshot-date 2026-04-10 \
  --region-name EMEA \
  --shell-source canonical \
  --allow-generated-shell-fallback \
  --powerpoint-mode skip
```

## Operating Rules

- Canonical shells are human- and Claude-authored PowerPoint assets, not generated `python-pptx` shells.
- Monthly content runs should never redesign the deck; they should only populate the canonical shell.
- If the canonical shell is missing, monthly runs must fail unless a human explicitly opts into generated-shell fallback.
- Any repaired or malformed shell should be discarded, not promoted.
- The shell contract is only valid if its slide-by-slide data coverage metadata passes `scripts/validate_sales_region_shell_contract.py`.
- PowerPoint population is structured-fill only: the JSON slot payload is the primary handoff, and the markdown fact pack exists to provide narrative guardrails and qualifications.

## Promotion Gate

Do not promote a shell unless:

- PowerPoint opens it without repair
- Claude pane binds successfully in PowerPoint
- placeholder text is editorial, not system-field language
- slide sequence matches the regional shell contract
- SimCorp branding is preserved

## Residual Risk

The remaining fragile point is still PowerPoint add-in automation. The control plane is now much stronger, but the shell itself must be a native, non-repaired PowerPoint artifact before the Claude PowerPoint lane can be trusted.
