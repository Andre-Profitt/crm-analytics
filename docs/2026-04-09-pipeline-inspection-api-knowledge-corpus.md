# Pipeline Inspection API Knowledge Corpus

Date: 2026-04-09
Org: `apro@simcorp.com`
Scope: Salesforce Pipeline Inspection API/UI behavior needed for D1/D2 delivery

## Why this exists

Pipeline Inspection is not a normal "just use REST" surface.

The platform exposes enough API to inventory and read Pipeline Inspection state, and enough UI to create and clone working views, but not enough documented or stable API to safely create arbitrary new territory views from scratch without probing the exact list-view compatibility rules.

This corpus is the working source of truth for PI until we finish the MD-territory rollout.

## Officially documented surfaces we can rely on

These official Salesforce docs are relevant and up to date:

- REST API Developer Guide, generic sObject row CRUD:
  - `https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/resources_sobject_retrieve.htm`
- LWC/UI API list-view references:
  - `https://developer.salesforce.com/docs/platform/lwc/guide/reference-get-list-infos-by-object-name.html`
  - `https://developer.salesforce.com/docs/platform/lwc/guide/reference-get-list-ui`
- Object reference entry points for the PI objects:
  - `https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_pipelineinspectionlistview.htm`
  - `https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_pipelineinspmetricconfig.htm`
  - `https://developer.salesforce.com/docs/atlas.en-us.object_reference.meta/object_reference/sforce_api_objects_pipelineinspectionsumfield.htm`

Important limitation:

- Salesforce's public docs are enough to confirm the objects and generic API surfaces exist.
- They do not document the Lightning-only PI view creation/association flow deeply enough for this org's rollout.
- The missing behaviors below are therefore based on live org probes, not doc inference.

## Live PI object model verified in this org

`PipelineInspectionListView` fields exposed by live `describe`:

- `Id`
- `ListViewId`
- `DateLiteralType`
- `StartDate`
- `EndDate`
- `IsSystemManaged`
- `ViewType`
- `SummaryField`
- `ChangePeriodLiteralType`
- `ChangePeriodStartDate`
- `UserId`
- `MarketSegments`

Related live objects:

- `PipelineInspMetricConfig`
- `PipelineInspectionSumField`

What this means:

- PI view state is a first-class sObject.
- A PI view is linked to an underlying list view through `ListViewId`.
- Metric and summary-field behavior is partly decomposed into related PI metadata objects.

## Working live PI seeds

These PI views are real and working in the org:

- Global ARR CFQ Forecast:
  - PI id: `4c2Tb0000003jobIAA`
  - ListViewId: `00BTb00000Ic82DMAR`
- APAC ARR CFQ Forecast:
  - PI id: `4c2Tb0000003jmzIAA`
  - ListViewId: `00BTb00000Ic7kTMAR`
- EMEA ARR CFQ Forecast:
  - PI id: `4c2Tb0000003jjlIAA`
  - ListViewId: `00BTb00000Ic77lMAB`
- NA ARR CFQ Forecast:
  - PI id: `4c2Tb0000003ji9IAA`
  - ListViewId: `00BTb00000Ic6JlMAJ`

Also useful as a compatibility proof:

- ACV Forecast EMEA_SC_Middle East:
  - PI id: `4c2QA0000000XK9YAM`
  - ListViewId: `00BQA00000GXOf32AH`

That Middle East seed is important because it proves PI can tolerate a sub-region filter on `Sales_Region__c`, not just top-level EMEA/APAC/NA segmentation.

## Working PI route

The real Lightning route for PI in this org is:

```text
/lightning/o/Opportunity/pipelineInspection?filterName=<value>
```

Verified behavior:

- `filterName=00BQA00000IGFYb2AP` loads `My Pipeline`
- `filterName=00BTb00000Ic82DMAR` loads the global ARR CFQ Forecast PI seed

Important caveat:

- arbitrary Opportunity list view ids do not automatically load a working PI view
- when the route target is not PI-registered/compatible, Lightning can fall back to the last valid PI seed instead of throwing a clean error

This makes route testing alone unreliable unless the returned heading and metrics are checked.

## UI-only behaviors verified live

From the working PI seed's `Pipeline Settings` menu:

- `Select Fields to Display`
- `Create New View`
- `Clone`
- `Rename`
- `Delete`
- `Sharing Settings`
- `Metrics Settings`

This is load-bearing:

- PI-native create/clone exists in Lightning UI
- this is the clearest supported path for making new PI views
- the REST API alone has not been enough to reproduce that flow for newly created territory list views

Also verified:

- the PI page's list selector only shows PI-enabled views, not every Opportunity list view
- direct Lightning record URLs for `PipelineInspectionListView/<id>` render `Unsupported Item`

## What fails today

### 1. Direct REST clone against new territory list views

Attempted pattern:

- clone existing PI seed
- override `ListViewId`
- POST to `/sobjects/PipelineInspectionListView`

Observed result:

- HTTP 400
- `INVALID_INPUT`
- `The filter type must be a supported type.`

This happened when targeting newly created CE / Canada / NA ex CA list views.

### 2. Fresh UI-API-created territory list views are not PI-ready

The new territory list views exist and are valid as normal Opportunity list views:

- `BoB ARR CFQ Forecast CE`
- `BoB ARR CFQ Forecast Canada`
- `BoB ARR CFQ Forecast NA ex CA`
- and the other MD geography slices

But on the PI route they land on:

- `Select Pipeline View`
- `No data to show`
- disabled filter controls

### 3. Standard list-page `Pipeline Inspection` action is not sufficient

From the CE Opportunity list page, clicking `Pipeline Inspection` did not create or bind a CE PI view.

Observed behavior:

- it reopened the existing global PI seed

So list-page context alone is not enough to mint a new PI binding.

## Compatibility pattern we have learned

The working PI-enabled ARR forecast list views all share an older list-view shape:

- columns include:
  - `IsPriorityRecord`
  - `Owner.Name`
  - `LastActivityInDays`
  - `PushCount`
  - `APTS_Forecast_ARR__c`
- columns do not include:
  - `Amount`
  - `Account_Unit__c`
  - `ZIMIT__zOwner__c`
  - `Consensus__cDaysSinceLastActivity__c`
- stage filter includes:
  - `7 - Opt Out`

The newly created MD territory list views currently differ:

- they use the newer UI-API-created column set
- they omit `IsPriorityRecord`
- they use `ZIMIT__zOwner__c` and `Consensus__cDaysSinceLastActivity__c`
- they include `Amount`
- they omit `7 - Opt Out`

Current best hypothesis:

- PI is rejecting or not recognizing the newer list-view shape as a supported PI source
- the issue is not that sub-region filters are impossible
- the issue is that the new list views were created through a different list-view layer than the older PI-compatible seeds

Evidence for the sub-region point:

- `ACV Forecast EMEA_SC_Middle East` is PI-enabled and uses `Sales_Region__c like '%Middle East%'`

## Current best path forward

### Path A: UI-native PI creation on the target territory route

This is the highest-probability next move.

Use the target PI route, for example:

```text
/lightning/o/Opportunity/pipelineInspection?filterName=00BTb00000Kqn1NMAR
```

Then use:

- `Pipeline Settings`
- `Create New View`

Why this matters:

- the create modal is real and working
- Salesforce may bind the new PI view to the current route context internally, even though the REST layer has rejected our manual clone attempts

### Path B: clone from a working PI seed inside PI UI

If Path A does not bind the target list view correctly:

- open a working compatible seed
- `Pipeline Settings -> Clone`
- inspect whether the post-save flow exposes a way to switch the underlying list/view context

### Path C: recreate territory list views through the older PI-compatible path

If the platform still rejects CE/Canada/NA ex CA:

- stop relying on the UI-API-created list views as PI seeds
- recreate territory list views using the older compatible shape, preserving:
  - `IsPriorityRecord`
  - `Owner.Name`
  - `LastActivityInDays`
  - stage set including `7 - Opt Out`

This is the fallback if the UI-native PI create flow still refuses the newer territory list views.

## What this corpus changes operationally

From this point on:

- do not assume `PipelineInspectionListView` POST is enough
- do not assume an Opportunity list view is PI-ready just because `/list` works
- do not trust PI route fallbacks without checking the page heading and KPI tiles
- prefer PI UI create/clone flows for the first working seed
- after the first working seed is proven, fan-out by territory

## Current next experiment

1. Open the CE PI route.
2. Use `Pipeline Settings -> Create New View`.
3. Save a CE-specific PI view.
4. Query `PipelineInspectionListView` again to discover the new PI id and bound `ListViewId`.
5. If CE works, repeat for Canada and NA ex CA.
6. Treat Patrick/Adam as the separate industry-split problem after the geography seeds are stable.

## Live probe commands

These commands are part of the verified working toolkit:

```bash
python3 scripts/manage_pi_views.py --target-org apro@simcorp.com list
python3 scripts/manage_opportunity_list_views.py --target-org apro@simcorp.com search --query 'BoB ARR CFQ Forecast'
sf data query --target-org apro@simcorp.com --json -q "SELECT Id, DeveloperName, Name, SobjectType FROM ListView WHERE Id = '00BTb00000Ic82DMAR'"
```

Useful direct routes:

```text
https://simcorp.lightning.force.com/lightning/o/Opportunity/pipelineInspection?filterName=00BTb00000Ic82DMAR
https://simcorp.lightning.force.com/lightning/o/Opportunity/pipelineInspection?filterName=00BTb00000Kqn1NMAR
https://simcorp.lightning.force.com/lightning/o/Opportunity/list?filterName=Book_of_Business_ARR_CFQ_Forecast_CE
```
