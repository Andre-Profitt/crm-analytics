# Analytics Intelligence CLI

This is the CLI-first intelligence layer above the direct CRM Analytics harnesses.

It does not replace the harness scripts. It resolves business asks into:

- semantic context
- surface routing
- review gates
- execution boundaries

## Commands

`scripts/analytics_intelligence.py` provides:

- `inventory`
- `resolve`
- `route`
- `review`
- `execute`
- `workflow`
- `validate`

## What This Layer Does

### `resolve`

Turns a free-form ask into:

- persona
- domain
- operation mode
- motion
- cadence
- likely question archetype
- candidate dashboard surfaces

This is driven by:

- `config/analytics_intelligence_profiles.json`
- `config/widget_decision_profiles.json`
- `config/context_registry.json`
- `config/commercial_operating_model.json`
- `config/sales_process_codification.json`
- `config/dashboard_autopilot_queue.json`

### `route`

Chooses the likely surface type:

- `salesforce_report`
- `salesforce_dashboard`
- `crma_dashboard`
- `tableau_workbook`
- `hybrid`

And then maps the request onto a lane sequence such as:

- `salesforce_data_profiles`
- `wave_data_validations`
- `live_inventory`
- `export_audits`
- `patch_guardrails`
- `dashboard_mutations`

When the resolved candidate surface carries a registered audit script, the router now prioritizes that audit in the `export_audits` lane instead of returning only a generic lane list.

### `review`

Builds an explicit checklist before mutation or sign-off.

Examples:

- `decision_fit`
- `metric_contract`
- `visual_fit`
- `action_layer`
- `patch_contract`
- `semantic_truth`
- `pre_export_snapshot`
- `post_mutation_reexport`

### `execute`

Runs only registered scripts behind a typed safety boundary:

- `read_only`
- `live_read`
- `mutating`

Mutating and destructive scripts are blocked unless explicitly allowed.

If a registered script advertises `supports_json_output: true`, the executor automatically requests `--json`, validates the returned envelope, and carries the parsed child result forward instead of only keeping a stdout excerpt.

### `workflow`

Builds one composite intelligence run:

- `resolve`
- `route`
- `review`
- optional safe execution

The safe execution mode is intentionally narrow right now. It supports structured read steps plus candidate-surface inspection when the workflow actually needs dashboard JSON and can run:

- selected `salesforce_data_profiles` scripts first
- selected `wave_data_validations` scripts when their args can be inferred safely
- live export only when a validation or review step requires exported dashboard state
- contract lint only for surface-inspection flows
- audit script when available and semantically relevant to the operation

When those child commands support `--json`, the workflow captures and summarizes their typed envelopes instead of only preserving stdout. That means one workflow result can now carry:

- profile summaries and artifact paths
- validation summaries and artifact paths
- export counts and output directory
- contract-lint file and violation counts
- audit pass/fail counts and artifact paths

This is the first step toward true CLI-to-CLI orchestration rather than a loose collection of commands.

Current structured audit coverage includes:

- `scripts/audit_forecast_revenue_motions.py`
- `scripts/audit_bdr_operating_system.py`
- `scripts/audit_account_intelligence.py`
- `scripts/audit_lead_management.py`
- `scripts/audit_customer_intelligence.py`
- `scripts/audit_lead_funnel.py`
- `scripts/audit_revenue_retention_health.py`
- `scripts/audit_executive_product_mix_industry.py`
- `scripts/audit_commercial_rhythm_control_tower.py`
- `scripts/audit_source_truth_executive_revenue.py`
- `scripts/audit_bdr_campaign_control.py`
- `scripts/audit_bdr_truth_layer.py`

Current structured profile coverage includes:

- `scripts/profile_role_structure.py`
- `scripts/profile_retention_semantics.py`
- `scripts/profile_bdr_operating_state.py`
- `scripts/profile_bdr_field_readiness.py`
- `scripts/profile_bdr_response_integrity.py`
- `scripts/profile_bdr_activity_model.py`
- `scripts/profile_bdr_quote_product_signals.py`
- `scripts/profile_retention_product_grain.py`
- `scripts/profile_renewal_outcomes.py`
- `scripts/profile_renewal_ownership.py`
- `scripts/profile_retention_owner_validation.py`

## Why This Is Structured This Way

This scaffold is aligned with the best current patterns for agentic analytics work:

- `ReAct`: interleave reasoning and tool use instead of generating first and validating later.
- `Reflexion`: keep explicit review and failure memory instead of rerunning blindly.
- `Language Agents as Optimizable Graphs`: use a small typed specialist graph instead of free-form multi-agent chatter.
- data-science agent benchmarks: evaluate full workflows, not only code generation.
- dbt-style semantics, exposures, and tests: resolve meaning and downstream usage before building surfaces.
- OpenLineage-style lineage thinking: know what a change affects before mutating production assets.

## Recommended Agent Graph

- `planner`
- `semantic_specialist`
- `surface_specialist`
- `critic`
- `executor`

That graph should call CLI commands, not hidden library abstractions.

## Practical Rule

Builders can still exist as baseline generators.

They should not be treated as the intelligence layer.

The intelligence layer is:

- resolve
- route
- review
- execute

## Example Flows

Resolve a business ask:

```bash
python3 scripts/analytics_intelligence.py resolve \
  --query "Manager action queue for deals needing intervention this week"
```

Route a surface:

```bash
python3 scripts/analytics_intelligence.py route \
  --query "Board narrative for forecast confidence and renewal health"
```

Review a mutating script:

```bash
python3 scripts/analytics_intelligence.py review \
  --script scripts/upgrade_executive_revenue_live.py
```

Run a safe local command through the executor boundary:

```bash
python3 scripts/analytics_intelligence.py execute \
  --script scripts/contract_lint.py -- --summary
```

Build a workflow plan:

```bash
python3 scripts/analytics_intelligence.py workflow \
  --query "Manager action queue for deals needing intervention this week"
```

Build a machine-readable workflow summary:

```bash
python3 scripts/analytics_intelligence.py workflow \
  --query "Manager action queue for deals needing intervention this week" \
  --json
```
