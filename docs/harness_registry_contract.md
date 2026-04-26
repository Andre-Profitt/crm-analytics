# Harness Registry Contract

This file is the first control-plane layer above the direct CLI scripts in this repo.

It does not replace the scripts. It makes their operating boundaries explicit so a planner, reviewer, or specialized agent can route work safely without guessing from filenames or docstrings.

## Why This Exists

- The repo already has strong direct CLI scripts.
- The missing piece was a machine-readable answer to: what lane is this script in, does it touch the live org, and how risky is it?
- `config/harness_registry.json` is the source of truth for that answer.
- `scripts/harness_registry.py` is the validator and query surface for that source of truth.

## Command Classes

- `read_only`: local-only commands that do not touch the live org.
- `live_read`: commands that read live Salesforce or Wave state but do not mutate it.
- `mutating`: commands that PATCH or DELETE live assets and require a reviewer or executor boundary.

## Active Lanes

- `intelligence_control`: top-level resolver, router, reviewer, and executor commands.
- `patch_guardrails`: local PATCH-safety checks such as `scripts/contract_lint.py`.
- `live_inventory`: live read-only inventory, export, and portfolio review scripts.
- `dashboard_mutations`: Wave PATCH and delete scripts for deployed dashboards.
- `export_audits`: local audits against exported `dashboard.json` snapshots.
- `wave_data_validations`: live or hybrid validation scripts that query Wave or combine local exports with live Salesforce evidence.
- `salesforce_data_profiles`: live profiling and semantic assessment scripts against Salesforce data.
- `local_intake`: local workbook and KPI intake scripts.

## Agent Specialization

If we add multi-agent orchestration on top of the harnesses, the clean split is by lane.

- `intelligence_control`: planner/router agent
- `patch_guardrails`: reviewer agent
- `live_inventory`: explorer agent
- `dashboard_mutations`: executor agent
- `export_audits`: audit agent
- `wave_data_validations`: truth-validation agent
- `salesforce_data_profiles`: profiling agent
- `local_intake`: intake agent

This is better than free-form multi-agent chatter because the lane boundary already matches operational risk.

## Target Result Envelope

The registry also defines the target CLI envelope for future script upgrades:

```json
{
  "status": "ok",
  "tool": "example_tool",
  "lane": "dashboard_mutations",
  "command_class": "mutating",
  "messages": [
    {
      "level": "info",
      "code": "example_code",
      "text": "Human-readable detail."
    }
  ],
  "artifacts": [
    {
      "kind": "json",
      "path": "output/example.json"
    }
  ]
}
```

Not every legacy script emits that yet. The registry is the contract we can retrofit toward.

Scripts that already emit this shape advertise it explicitly with `supports_json_output: true` in `config/harness_registry.json`.

## Explicit Exclusions

Two wrappers are intentionally excluded from the supported direct API surface:

- `scripts/run_dashboard_autopilot.py`
- `scripts/run_focus_loop.py`

They orchestrate legacy builder entries and should not be part of the active direct Wave/API contract until that dependency is removed.

## Immediate Retrofit Priorities

- Add `--json` or JSON-sidecar result envelopes to the live inventory and mutation scripts.
- Move duplicate auth code onto a shared helper path with consistent exit handling.
- Add structured warning and error codes outside the existing PATCH-contract validator.
- Add dry-run support for mutating scripts before broader orchestration is layered on top.
