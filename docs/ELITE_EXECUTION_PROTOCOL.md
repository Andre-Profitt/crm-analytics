# Elite Execution Protocol

This protocol defines how focused dashboard work should run before any build
loop starts.

Use it with:

- `/Users/test/crm-analytics/docs/CONSULTANT_GRADE_CRMA_PLAYBOOK.md`
- `/Users/test/crm-analytics/docs/COMMERCIAL_OPERATING_RHYTHM.md`
- `/Users/test/crm-analytics/docs/generated/SALES_PROCESS_OPERATING_MODEL_2026-03-11.md`
- `/Users/test/crm-analytics/docs/WIDGET_DECISION_LIBRARY.md`
- `/Users/test/crm-analytics/config/widget_decision_profiles.json`
- `/Users/test/crm-analytics/config/commercial_operating_model.json`

## 1. Research First

Do not start with implementation.

Every dashboard cycle should begin by answering:

1. Which persona is this for?
2. Which operating decision should this dashboard change?
3. Which motion or workflow owns that decision?
4. Which metrics are source-truth and which are still semantic-risky?
5. Which widget forms are actually correct for this persona and question?

If those are not clear, do not build yet.

## 2. Use Three Reference Layers

### Platform truth

- CRM Analytics implementation limits
- Salesforce workflow realities
- Tableau/visual best-practice patterns

### SimCorp operating truth

- sales handbook
- commercial rhythm
- role structure
- motion ownership
- renewal semantics

### Live org truth

- actual field coverage
- actual ownership structure
- actual dashboard usage and defects
- actual data quality gaps

All three must agree closely enough before a metric or workflow is trusted.

## 3. Build Questions, Not Charts

For each page, declare:

- audience
- cadence
- business question
- comparator
- required action
- shared drill target

If a widget does not materially help answer the page question, remove it.

## 4. Cross-Check Against The Existing Contract

Before changing a dashboard, re-read the relevant notes:

- consultant-grade playbook
- commercial operating rhythm
- sales process operating model
- widget decision library
- most recent domain-specific audit

The build should refine the contract, not drift away from it.

## 5. Demand Real-World Sophistication

The standard is:

- role-based operating systems
- explicit handoffs
- semantic confidence
- action queues
- accountable drill paths

The standard is not:

- chart galleries
- generic KPI strips
- executive language on manager pages
- metrics that mix incompatible value fields without disclosure

## 6. The Loop

The correct loop is:

1. Research and frame the problem
2. Reconcile with playbook and operating model
3. Audit the live state
4. Design the next change in words first
5. Implement
6. Export live state
7. Audit again
8. Only then continue

## 7. Sophistication Checks

Each focused run should explicitly ask:

- Are we using the correct grain?
- Are we using the correct value field?
- Are we using the correct persona language?
- Is this widget the best form or just an acceptable one?
- Does this page create a real operating rhythm?
- Does this dashboard route to the right next surface?

If the answer is weak, pause and redesign before more code.
