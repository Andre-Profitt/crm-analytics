# Harness-First AI OS Rollout Plan

> **For agentic workers:** REQUIRED: execute this plan in order, keep checkbox state current, and do not introduce parallel orchestration systems. Extend the existing harness and workflow surface instead.

**Goal:** Turn the current CRM Analytics harness stack into a controlled AI OS with planning, memory, evaluation, and mutation gates, while preserving the existing CLI-first execution model.

**Architecture:** Harness-first, CLI-to-CLI, Markdown for human synthesis, JSON for machine state. The system remains centered on `harness_registry.json`, `analytics_intelligence.py`, and the existing executor scripts. No MCP runtime. No generic multi-agent framework. No `build_*.py` work unless explicitly requested.

**Tech Stack:** Python 3, existing JSON result envelopes, filesystem-backed artifacts, direct Wave API / `sf` CLI integrations, deterministic validation logic.

**Research Basis:** ReAct and SWE-agent/OpenHands for constrained planning, CRITIC/Phoenix/Pydantic Evals for evaluator design, and MemGPT/Letta for memory hierarchy. These are donor patterns, not runtime dependencies.

---

## Non-Negotiable Rules

- [x] Keep `scripts/analytics_intelligence.py` as the single orchestration entrypoint.
- [x] Keep `config/harness_registry.json` as the control surface for lanes, risk, and execution policy.
- [x] Keep Markdown and JSON artifact roles separate.
- [x] Do not introduce a second router alongside the existing resolver/router logic.
- [x] Do not allow a `mutating` harness to appear as the first planned step.
- [x] Do not make evaluator logic LLM-first; deterministic checks stay primary.
- [x] Do not modify or rely on legacy `build_*.py` files.

---

## File Map

| File | Action | Responsibility |
| --- | --- | --- |
| `config/harness_registry.json` | Modify | Extend script metadata into a planner-safe policy graph |
| `scripts/harness_registry.py` | Modify | Validate new policy graph fields and expose them in `describe` |
| `config/agent_os_core_memory.json` | Create | Stable repo/org facts for planner and evaluator |
| `scripts/run_memory.py` | Create | File-backed run memory: `search`, `record`, `show` |
| `tests/test_run_memory.py` | Create | Memory CLI and scoring tests |
| `config/harness_planner_profiles.json` | Create | Lane order, evidence requirements, planner scoring weights |
| `scripts/harness_planner.py` | Create | Evidence-first planner over registry, route, and memory hits |
| `tests/test_harness_planner.py` | Create | Planner sequencing and safety tests |
| `config/plan_evaluator_profiles.json` | Create | Evidence-to-artifact mapping and evaluator severity rules |
| `scripts/plan_evaluator.py` | Create | Deterministic plan/evidence evaluator |
| `tests/test_plan_evaluator.py` | Create | Evaluator verdict and blocking-finding tests |
| `scripts/analytics_intelligence.py` | Modify | Integrate memory, planner, and evaluator into `workflow` |
| `tests/test_analytics_intelligence.py` | Modify | Preserve workflow contract while adding planner/evaluator coverage |
| `scripts/builder_brain.py` | Modify | Carry planning/evaluation artifacts into handoff/build packages |
| `tests/test_builder_brain.py` | Modify | Verify planning context propagation in package/handoff output |
| `scripts/wave_patch_executor.py` | Modify | Soft gate first, then hard gate on evaluation `pass` |
| `scripts/salesforce_dashboard_executor.py` | Modify | Soft gate first, then hard gate on evaluation `pass` |
| `tests/test_wave_patch_executor_cli.py` | Modify | Evaluation gate behavior for worklist/deploy |
| `tests/test_salesforce_dashboard_executor_cli.py` | Modify | Evaluation gate behavior for apply/complete |

---

## Artifact Contract

All new autonomous runs should live under:

`output/agent_runs/<run_id>/`

Required structure:

- [ ] `01_goal/goal.md`
- [ ] `02_memory/memory_hits.json`
- [ ] `03_plan/plan.json`
- [ ] `04_steps/<step_id>/...`
- [ ] `05_evaluation/evaluation.json`
- [ ] `06_handoff/handoff.md`
- [ ] `07_execution/execution_result.json`

Artifact rules:

- [ ] Markdown is for goal framing, evidence summary, and handoff narrative.
- [ ] JSON is for planner state, evaluator verdicts, and execution metadata.
- [ ] Memory index records paths to artifacts, not full copied payloads.

---

## Phase 1: Control Contract

### Task 1: Extend `harness_registry.json`

**Files:**

- Modify: `config/harness_registry.json`

- [x] Add per-script fields:
  - `policy_profile`
  - `approval_required`
  - `allowed_predecessor_lanes`
  - `allowed_successor_lanes`
  - `evidence_types_produced`
  - `memory_tags`
- [x] Use lane defaults:
  - `intelligence_control` -> `research`
  - `patch_guardrails` -> `audit`
  - `live_inventory` -> `audit`
  - `export_audits` -> `audit`
  - `wave_data_validations` -> `audit`
  - `salesforce_data_profiles` -> `audit`
  - `native_surface_authoring` -> `authoring`
  - `dashboard_mutations` -> `mutation_review`
  - `local_intake` -> `research`
- [x] Mark every `mutating` script with `approval_required: true`.
- [x] Mark mixed-mode native authoring scripts conservatively with `approval_required: true`.
- [x] Keep lane transitions narrow and intentional.

### Task 2: Extend registry validation

**Files:**

- Modify: `scripts/harness_registry.py`
- Modify: `tests/test_harness_registry.py`

- [x] Add `ALLOWED_POLICY_PROFILES`.
- [x] Validate presence and type of all new fields.
- [x] Validate predecessor/successor lane IDs.
- [x] Reject duplicate evidence types and memory tags.
- [x] Reject any `mutating` script with `approval_required: false`.
- [x] Reject any `destructive` script with `approval_required: false`.
- [x] Expose the new fields in `describe`.
- [x] Keep `inventory` output stable.

**Acceptance:**

- [x] `python3 scripts/harness_registry.py validate --json` returns `ok`.
- [x] `describe` output includes the new policy metadata.

---

## Phase 2: File-Backed Memory

### Task 3: Add core memory

**Files:**

- Create: `config/agent_os_core_memory.json`

- [x] Add stable facts from repo instructions:
  - target org
  - API version
  - fiscal year start
  - dashboard contract rules
  - mutation boundary rules
- [x] Keep it small and declarative.

### Task 4: Add `run_memory.py`

**Files:**

- Create: `scripts/run_memory.py`
- Create: `tests/test_run_memory.py`

- [x] Implement CLI commands:
  - `search`
  - `record`
  - `show`
- [x] Use storage:
  - `output/agent_memory/run_index.jsonl`
  - `output/agent_memory/runs/<run_id>.json`
- [x] Use deterministic ranking only:
  - normalized goal token overlap
  - domain boost
  - persona boost
  - operation boost
  - tag boost
  - optional failure-reason boost
- [x] Keep result envelopes consistent with existing CLIs.

**Acceptance:**

- [x] `record` writes both a per-run JSON file and an index entry.
- [x] `search` returns deterministic ranked results.
- [x] `show` returns a stored run or a structured `error`.

---

## Phase 3: Planner

### Task 5: Add planner profiles

**Files:**

- Create: `config/harness_planner_profiles.json`

- [x] Define:
  - preferred lane order by operation
  - required evidence by operation
  - score boosts for candidate-linked scripts
  - score boosts for memory reuse
  - maximum default number of steps

### Task 6: Add `harness_planner.py`

**Files:**

- Create: `scripts/harness_planner.py`
- Create: `tests/test_harness_planner.py`

- [x] Implement one command: `plan`.
- [x] Reuse `analytics_intelligence` resolution and routing logic rather than duplicating it.
- [x] Build the smallest evidence-first sequence that covers required evidence.
- [x] Emit:
  - `recommended_sequence`
  - `required_evidence`
  - `mutation_candidates`
  - `stop_conditions`
  - `safe_execution_supported`
  - `planner_notes`
- [x] Keep mutation candidates separate from the evidence-gathering sequence.

**Planner hard rules:**

- [x] No `mutating` script inside `recommended_sequence`.
- [x] First step must be `read_only` or `live_read`.
- [x] All lane transitions must be allowed by the registry.
- [x] If a script requires local export, an export-producing step must come earlier.
- [x] Prefer candidate-linked audit or validation scripts where available.

**Acceptance:**

- [x] Forecast/dashboard mutation asks plan to export, lint, and audit before mutation candidates appear.
- [x] The planner never emits a `mutating` first step.

---

## Phase 4: Evaluator

### Task 7: Add evaluator profiles

**Files:**

- Create: `config/plan_evaluator_profiles.json`

- [x] Define:
  - required evidence by operation
  - artifact type to evidence mapping
  - finding code severity mapping

### Task 8: Add `plan_evaluator.py`

**Files:**

- Create: `scripts/plan_evaluator.py`
- Create: `tests/test_plan_evaluator.py`

- [x] Implement one command: `evaluate`.
- [x] Evaluate in this order:
  1. plan integrity
  2. required evidence presence
  3. deterministic contract checks
  4. mutation readiness
- [x] Emit only:
  - `pass`
  - `needs_more_evidence`
  - `fail`
- [x] Return:
  - `rule_checks`
  - `blocking_findings`
  - `required_next_steps`
  - `evidence_gaps`
  - `mutation_ready`

**Evaluator hard rules:**

- [x] Missing required evidence -> `needs_more_evidence`
- [x] Invalid lane transition -> `fail`
- [x] `mutating` script inside plan sequence -> `fail`
- [x] Blocking lint or dashboard-contract violation -> `fail`
- [x] Dataset-integrity evidence missing on truth-sensitive flows -> `needs_more_evidence`

---

## Phase 5: Workflow Integration

### Task 9: Integrate memory and planner into `workflow`

**Files:**

- Modify: `scripts/analytics_intelligence.py`
- Modify: `tests/test_analytics_intelligence.py`

- [x] Keep `workflow` as the single orchestration command.
- [x] Replace inline planned-command heuristics with planner output.
- [x] Preserve current top-level workflow response shape.
- [x] Add safe new fields:
  - `workflow.memory_hits`
  - `workflow.plan`
  - `workflow.summary.planner_notes`
- [x] Keep existing step names where possible:
  - `export_live_assets`
  - `contract_lint`
  - `audit_surface`

### Task 10: Integrate evaluator into `workflow --execute-safe`

**Files:**

- Modify: `scripts/analytics_intelligence.py`
- Modify: `tests/test_analytics_intelligence.py`

- [x] Run evaluator after safe execution completes.
- [x] Write `evaluation.json` into the workflow artifact tree.
- [x] Add:
  - `workflow.evaluation`
  - `workflow.summary.evaluation_verdict`
- [x] Map evaluator verdicts to workflow status:
  - `pass` -> preserve step-derived status
  - `needs_more_evidence` -> downgrade `ok` to `warn`
  - `fail` -> force `error`
- [x] Record runs in memory after evaluation.

**Acceptance:**

- [x] Existing workflow tests still pass after additive field changes.
- [x] Safe workflow runs always produce `plan.json` and `evaluation.json`.

---

## Phase 6: Soft Propagation To Handoffs

### Task 11: Carry planning context through builder-brain output

**Files:**

- Modify: `scripts/builder_brain.py`
- Modify: `tests/test_builder_brain.py`

- [x] Accept optional `--plan` and `--evaluation`.
- [x] Include planning context in build package and handoff output:
  - `plan_path`
  - `evaluation_path`
  - `evaluation_verdict`
- [x] Do not hard-block handoffs yet.

**Acceptance:**

- [x] Package and handoff artifacts include `planning_context` when provided.

---

## Phase 7: Hard Mutation Gates

### Task 12: Gate Wave patch execution

**Files:**

- Modify: `scripts/wave_patch_executor.py`
- Modify: `tests/test_wave_patch_executor_cli.py`

- [x] Add `--evaluation` support.
- [x] Require evaluation `pass` for mutation-ready `worklist` and `deploy`.
- [x] Add an explicit override flag for operator-only bypass.
- [x] Emit a warning code when bypass is used.

### Task 13: Gate native dashboard execution

**Files:**

- Modify: `scripts/salesforce_dashboard_executor.py`
- Modify: `tests/test_salesforce_dashboard_executor_cli.py`

- [x] Add `--evaluation` support.
- [x] Require evaluation `pass` for `apply` and `complete`.
- [x] Add an explicit override flag for operator-only bypass.
- [x] Emit a warning code when bypass is used.

**Acceptance:**

- [x] Mutation-ready paths refuse to proceed without evaluator `pass`.
- [x] Manual override remains possible but explicit and noisy.

---

## Phase 8: Stabilization

### Task 14: Tune planning and memory reuse

**Files:**

- Modify: `config/harness_planner_profiles.json`
- Modify: `scripts/run_memory.py`
- Modify: `scripts/harness_planner.py`

- [x] Penalize sequences tied to repeated failure codes.
- [x] Boost reuse of successful sequences when route and evidence needs match.
- [x] Keep the scoring deterministic.

### Task 15: Operational review

**Files:**

- Create or update: `docs/generated/` rollout notes as needed

- [x] Record implementation findings and any policy adjustments.
- [x] Document any executor bypasses that proved necessary.
- [x] Do not add new architecture in this phase; tune only what already exists.

---

## Execution Order

- [x] Phase 1: Control Contract
- [x] Phase 2: File-Backed Memory
- [x] Phase 3: Planner
- [x] Phase 4: Evaluator
- [x] Phase 5: Workflow Integration
- [x] Phase 6: Soft Propagation To Handoffs
- [x] Phase 7: Hard Mutation Gates
- [x] Phase 8: Stabilization

No later phase should begin until the current phase passes its acceptance checks.

---

## Definition Of Success

- [x] The system chooses evidence-gathering steps before mutation candidates.
- [x] Every autonomous run leaves behind durable plan and evaluation artifacts.
- [x] Similar requests converge on consistent harness sequences over time.
- [x] Mutation readiness becomes explicit, artifact-backed, and testable.
- [x] The repo remains CLI-first, harness-first, and understandable by operators.
