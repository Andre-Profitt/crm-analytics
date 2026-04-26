# Modern CRMA Platform Architecture

**Date:** March 10, 2026
**Purpose:** Define the target technical architecture for a modern, enterprise-grade CRM Analytics system at SimCorp.

## Executive Summary

Python should **not** be the primary build model for this program.

The target architecture is:

- `CRM Analytics native` for datasets, dashboard interactions, XMD, record links, security, and embedded analytics behavior
- `Salesforce metadata` for source control, deployment, and environment promotion
- `Lightning / LWC` for richer application workflows and embedded action surfaces
- `AI-native Salesforce capabilities` for predictions, explanations, and assistant workflows
- `Python` only for supporting automation, external ingestion, backfills, QA, and selective feature engineering

This reduces brittleness, improves governability, and gets us closer to a true operating system instead of a pile of generated dashboard JSON.

## Why A Python-First CRMA Build Is Brittle

Python is useful, but it becomes the wrong center of gravity when it owns too much of the analytics product.

### Main failure modes

1. **Drift from production**
   - Generated local dashboards can diverge from live dashboards.
   - We already saw this in the revenue workstream: local and live are materially different.

2. **Harder admin ownership**
   - Admins and analysts can work with recipes, dataflows, metadata, XMD, and dashboard JSON in Salesforce-native workflows.
   - Fewer people can safely maintain a custom Python generation framework.

3. **Weak metadata discipline**
   - CRMA assets are metadata-first products.
   - If dashboards are only generated ad hoc, versioning and promotion become less transparent.

4. **Action layer underbuilt**
   - Python can generate widgets, but it is not the right primary tool for embedded app workflow, Lightning pages, or interactive Salesforce action design.

5. **AI layering becomes awkward**
   - In the AI era, the value is not just generating JSON. The value is combining trusted metrics, native action surfaces, predictions, and guided workflows.
   - That belongs mostly in Salesforce-native layers.

## What The Modern Stack Should Be

## 1. Data And Semantic Layer

Use CRM Analytics-native assets wherever possible:

- `Recipes`
- `Dataflows`
- `Replicated datasets`
- governed dataset grain
- stable shared metric definitions

This layer should own:

- funnel definitions
- forecast categories
- revenue motions
- account and segment hierarchies
- health score inputs
- NRR / GRR / churn logic
- delivery model and product hierarchy

Python remains acceptable here only when:

- external data ingestion is required
- a one-time backfill is needed
- a native recipe/dataflow cannot express the logic cleanly
- a feature engineering job is too advanced for native tooling

## 2. Analytics Metadata Layer

All major CRM Analytics assets should be versioned as Salesforce metadata in `force-app`.

That includes:

- dashboards
- recipes
- dataflows
- datasets
- XMD
- templates where relevant

The repo already has a Salesforce DX project at [sfdx-project.json](/Users/test/crm-analytics/sfdx-project.json), but `force-app` is effectively unused today.

That needs to change.

This layer should become the deployment source of truth for:

- `WaveDashboard`
- `WaveDataflow`
- `WaveRecipe`
- `WaveDataset`
- `WaveXmd`

## 3. Experience Layer

Use CRM Analytics dashboards for:

- KPI surfaces
- trends
- bridges
- drillable comparisons
- exception queues
- analytic filtering and state

Use Lightning and LWC for:

- guided workflows
- richer action centers
- cross-dashboard navigation shells
- embedded analytics on Account / Opportunity / Lead / Contract records
- custom triage tables when native compare tables are too limiting

This is where the system becomes more than reporting.

## 4. AI Layer

Use AI on top of a governed semantic layer, not instead of one.

Primary options:

- `Einstein Discovery` for native predictive models
- `Salesforce Models API / Agentforce` for generative and guided experiences
- recommendation surfaces backed by trusted metrics and prediction outputs

AI should support:

- renewal risk
- win / slip probability
- whitespace recommendations
- next-best-action
- manager summaries and exception explanations

But AI should never be allowed to define the core metrics.

## 5. Automation And QA Layer

This is where Python still fits well.

Python should own:

- org inspection
- schema audits
- screenshot generation
- dashboard QA and linting
- migration helpers
- one-time asset extraction
- external feature computation
- dataset sanity checks

Python is strongest as the tooling and automation layer, not the core application runtime.

## Salesforce-Native Building Blocks We Should Use More

## CRM Analytics native

- dashboard advanced editor
- bindings
- faceting
- XMD
- recipes
- dataflows
- compare tables
- record links and record actions
- embedded dashboards in Lightning pages

## Lightning platform

- Lightning app pages
- LWCs
- Flexipages
- custom navigation and shells
- reusable action components

## AI-native Salesforce

- Einstein Discovery
- Models API
- Agentforce patterns where they genuinely improve workflow

## Recommended Architecture For SimCorp

### Layer A: Source Systems

- Salesforce standard and custom objects
- Forecasting objects
- Activities
- Contracts / renewals
- product and line item data
- optional finance, support, telemetry, and marketing systems

### Layer B: Governed Metric Products

Shared curated datasets for:

- demand
- revenue / forecast
- customer health
- retention
- product / GTM
- productivity

These become reusable building blocks, not one-off dashboard feeds.

### Layer C: Domain Dashboards

Native CRMA dashboards by domain and persona:

- executive
- manager
- individual
- analyst

### Layer D: Embedded Workflow

Lightning pages and LWCs embedding:

- Account 360
- opportunity risk context
- lead / BDR queue
- renewal portfolio

### Layer E: AI Copilot / Prediction Layer

- predictive scoring
- next-best-action
- narrative summaries
- guided follow-up workflows

## When Python Is Still The Right Tool

Use Python when:

- building a temporary migration bridge
- extracting live dashboard definitions for diffing
- validating org schema and field coverage
- generating screenshots and review artifacts
- performing advanced offline modeling
- computing features not practical in native CRMA tooling
- integrating non-Salesforce data sources before a better pipeline exists

Do **not** default to Python when:

- a recipe or dataflow can express the transformation cleanly
- the requirement is dashboard interaction behavior
- the requirement is record-page embedding
- the requirement is standard metadata promotion
- the requirement is action workflow inside Salesforce

## Migration Strategy From The Current Repo

### Phase 1: Stop The Drift

1. Pull live CRMA assets into source control as metadata where possible.
2. Treat live dashboards as the current truth, not just Python output.
3. Preserve Python scripts only as migration and audit tools during transition.

### Phase 2: Re-center On Native CRMA

1. Move stable dataset logic into recipes/dataflows where appropriate.
2. Version XMD and dashboards as metadata.
3. Standardize shared filters, grain, and metric contracts.

### Phase 3: Add Richer App Surfaces

1. Build Lightning shells for executive and manager navigation.
2. Embed account, opportunity, and renewal analytics in record workflows.
3. Use LWC where native dashboard widgets are not enough.

### Phase 4: Operationalize AI

1. Surface native prediction scores in operating dashboards.
2. Add assistant-style summary and triage flows only after metric trust is stable.
3. Track adoption and outcome lift from recommendations.

## Recommended Default Rule

For every new capability, choose in this order:

1. `Can CRM Analytics native handle it cleanly?`
2. `Should it be versioned and deployed as Salesforce metadata?`
3. `Does it need Lightning/LWC because it is really an app workflow?`
4. `Does AI improve the workflow after the metric logic is stable?`
5. `Only then ask whether Python is needed.`

If Python is the first answer, we are probably designing the wrong system.

## Current Repo Implications

The repo already has the ingredients for a better model:

- Salesforce DX project: [sfdx-project.json](/Users/test/crm-analytics/sfdx-project.json)
- MCP endpoints for Salesforce and CRM Analytics: [.mcp.json](/Users/test/crm-analytics/.mcp.json)
- rich CRMA research and review docs
- live org access and dashboard inventory

What is missing is the operating shift:

- from generated dashboards to governed metadata
- from dashboard-only thinking to Lightning-embedded workflow
- from Python-first to native-platform-first

## Bottom Line

The modern approach is **not** to abandon Python completely.

The modern approach is to demote Python to a support role and let:

- CRM Analytics own analytics behavior
- Salesforce metadata own deployment
- Lightning own workflow
- AI own explanation and guided assistance

That is the architecture we should build toward.
