# Handoff: Excel → PowerPoint Deck Generation via Claude

Date: 2026-04-10

## Goal

Take 9 Sales Director Excel workbooks (12 tabs each, live Salesforce data) and generate polished executive PowerPoint presentations. One deck per director.

## The Problem to Solve

The prior session built the data layer (Excel workbooks). This session needs to build the presentation layer. The prior agent explored several approaches but did NOT validate any of them end-to-end. Treat everything below as leads to investigate, not confirmed facts.

## What Exists (verified)

9 Excel workbooks at `output/director_data_dumps/2026-04-10/`:

```
Sales Director Data - {name} ({territory}).xlsx
```

Each has 12 tabs: Scorecard, Pipeline Detail, Q1 Review, Rep Performance, Won-Lost, Sources & Lineage, Q2 Outlook, Commercial Approval, Renewals & Retention, Risk Register, Data Quality, Quota & Targets (placeholder).

SimCorp PPT template at:

```
~/archive/simcorp-deck-agent-backup/reference-decks/SimCorp_PPT_Template.pptx
```

Anthropic SDK v0.84.0 installed. `ANTHROPIC_API_KEY` is NOT set (commented out in .zshrc). Check `~/secure-backup/` or Anthropic console.

## Approaches to Investigate (NOT validated)

The prior agent found several possible paths. None were tested. Start by investigating which actually works before committing to an architecture.

### 1. Claude API Skills (pptx/xlsx)

The SDK has `client.beta.skills` and `client.beta.messages.create()` accepts a `container` param with skills. The prior agent found these types in the SDK source:

```
/site-packages/anthropic/types/beta/beta_container_params.py  → BetaContainerParams(id, skills)
/site-packages/anthropic/types/beta/beta_skill_params.py      → BetaSkillParams(skill_id, type, version)
```

**Unverified claims to test:**

- Can you pass `skill_id="pptx"` and `skill_id="xlsx"` as anthropic-type skills?
- What beta flags are actually required? (prior agent guessed: `code-execution-2025-08-25`, `files-api-2025-04-14`, `skills-2025-10-02`)
- Does container reuse actually work for shared context between xlsx analysis and pptx generation?
- How do you extract the generated file from the response? The prior agent guessed `container_upload` block type but didn't verify.
- What are the file size limits for uploading Excel workbooks?

**How to investigate:**

- Read the Anthropic API docs: `https://docs.anthropic.com/en/docs/`
- Check `client.beta.skills.list()` with an API key to see what built-in skills exist
- Try a minimal test: upload a small xlsx, ask Claude to read it, see what happens
- Check the SDK source at `/Users/test/Library/Python/3.13/lib/python/site-packages/anthropic/` for response types

### 2. Claude for Excel/PowerPoint Add-ins

Anthropic ships Office add-ins (launched 2025-2026). The prior agent found marketplace references but did NOT check if they're installed or how they work programmatically.

**Things to investigate:**

- Are the add-ins installed? Check Excel → Insert → Add-ins, or search Office solution packages at `~/Library/Group Containers/UBF8T346G9.Office/SolutionPackages/`
- Do the add-ins expose any programmatic interface (API, webhooks, Office JS commands)?
- Can the add-ins be driven via Office Scripts (TypeScript automation in Excel Online)?
- Is there a way to automate the add-in sidebar via Office JS API?
- The March 2026 "shared context" feature — how does it work technically? Is it just container reuse or something deeper?

### 3. MCP Servers for Office Files

Community-built MCP servers exist:

- `@negokaz/excel-mcp-server` (npm v0.12.0) — reads/writes .xlsx
- `mcp-powerpoint` (npm v0.1.3) — PPTX manipulation
- `Office-PowerPoint-MCP-Server` (GitHub, 32 python-pptx tools)

**Not investigated:** Whether any of these are good enough for production-quality SimCorp-branded decks, or if they're just basic wrappers.

### 4. Playwright → Office 365 Online

Could open Excel Online / PowerPoint Online in a browser and drive the Claude add-in sidebar via Playwright automation. Fragile but would use the full add-in capabilities.

### 5. Direct python-pptx (fallback)

The repo already has `scripts/build_nam_deck.py` which generates per-director decks using python-pptx + the SimCorp template. It works but produces template-filled decks, not insight-driven narratives. Could be enhanced with Claude-generated narrative text injected into the slides.

## What the Prior Agent Got Wrong or Didn't Verify

1. **The API code samples are guesses.** The prior agent read SDK type definitions but never made an actual API call. The beta flags, file upload format, response structure, and skill IDs are all unverified.
2. **The prior agent assumed container_upload is the response type for generated files.** This was not confirmed.
3. **The add-in architecture was not investigated.** The agent searched local files but didn't find the Claude add-in specifically — only Microsoft Copilot packages.
4. **No end-to-end test was run.** Zero generated PowerPoint files from any approach.

## Recommended Investigation Order

1. **Get the API key working first.** Nothing else matters without it.
2. **List available skills** via `client.beta.skills.list()` — confirm `pptx` and `xlsx` exist.
3. **Run a minimal test** — upload a tiny Excel file, ask Claude to read a cell, verify the round-trip.
4. **Then test pptx generation** — ask Claude to create a 1-slide deck, verify you get a file back.
5. **Only then** design the full pipeline and prompt strategy.

If the API skills approach doesn't work or the quality isn't good enough, fall back to investigating the MCP server route or the enhanced python-pptx route.

## Files Reference

| File                                                   | Purpose                                               |
| ------------------------------------------------------ | ----------------------------------------------------- |
| `scripts/director_data_helpers.py`                     | Shared constants, auth, query helpers                 |
| `scripts/extract_director_data.py`                     | Phase 1: SF → JSON cache                              |
| `scripts/build_director_workbooks.py`                  | Phase 2: JSON → Excel workbooks (1,500+ lines)        |
| `scripts/build_nam_deck.py`                            | Existing deck builder (python-pptx, works but basic)  |
| `config/sales_director_md1_presets.json`               | 9 director filter definitions                         |
| `docs/specs/2026-04-10-director-data-dump-design.md`   | Full spec (12 tabs, all data sources, 566 lines)      |
| `docs/2026-04-10-dashboard-report-knowledge-corpus.md` | Org metadata, thresholds, process rules (1,769 lines) |

## Director Workbooks

| Director          | Territory            | Open Opps | Tabs with Data |
| ----------------- | -------------------- | --------- | -------------- |
| Jesper Tyrer      | APAC                 | 143       | 11 of 12       |
| Sarah Pittroff    | Central Europe       | 417       | 11 of 12       |
| Francois Thaury   | Southern Europe      | 157       | 11 of 12       |
| Dan Peppett       | UK & Ireland         | 167       | 11 of 12       |
| Christian Ebbesen | NL & Nordics         | 376       | 11 of 12       |
| Mourad Essofi     | Middle East & Africa | 84        | 11 of 12       |
| Megan Miceli      | Canada               | 131       | 11 of 12       |
| Patrick Gaughan   | NA Asset Management  | 176       | 11 of 12       |
| Adam Steinhaus    | Pension & Insurance  | 93        | 11 of 12       |

Tab 12 (Quota & Targets) is a placeholder awaiting Finance data.

## Brand Reference

- Sales Handbook V4: `~/Downloads/Sales Handbook V4.pptx` (57 slides of process context)
- Authority Policy: `~/Downloads/Nov 2024 Authority Policy 2.pdf` (approval thresholds)
- SimCorp PPT Template: 34 master slide layouts
- Existing deck output from prior sessions: `output/sales_director_monthly_runs/2026-04-09/` (9 per-director decks from build_nam_deck.py)
