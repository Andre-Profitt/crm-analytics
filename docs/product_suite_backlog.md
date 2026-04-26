# Product Suite Backlog

## Goal
Build the Product app as a connected CRM Analytics suite rather than extending the current single dashboard indefinitely.

## Target Dashboards

1. `Executive Product Mix & Industry`
   - Questions:
     - Where are ARR and install base concentrated?
     - How do banking, insurance, asset management, pensions, wealth, and servicing differ?
     - Where are whitespace and SaaS mix most material?
   - Status:
     - Implemented as first product-suite dashboard

2. `Industry & Business Segment Deep Dive`
   - Questions:
     - How do industries and internal segments differ in mix, attach, and whitespace?
     - Which vertical / segment combinations are structurally underpenetrated?
   - Required data:
     - normalized industry
     - segment
     - product cluster
     - cohort/benchmark logic

3. `Whitespace & Cross-Sell Action Center`
   - Questions:
     - Which accounts should sellers pursue next?
     - Which product motion is realistic and how much is it worth?
   - Required data:
     - account-product-month backbone
     - whitespace score fact
     - next-best-action outputs
     - actionable compare tables

4. `SaaS vs Non-SaaS Transition`
   - Questions:
     - Which accounts and industries should drive the next SaaS / transition push?
     - Where is services attach or cloud migration most relevant?
   - Required data:
     - delivery model
     - renewal timing
     - services attach
     - monthly history for transition trend

## Data Foundation Work

### Phase 0
- Normalize `Industry` into stable executive cohorts.
- Normalize `PortfolioCluster` from product-family values.
- Normalize `DeliveryModel` from current Salesforce delivery flags.
- Lock shared filters:
  - `IndustryVertical`
  - `Segment`
  - `DeliveryModel`
  - `UnitGroup`

### Phase 1
- Build `account x product cluster x month` snapshot.
- Separate:
  - installed ARR
  - open expansion ARR
  - open renewal ARR
  - SaaS ARR
  - non-SaaS ARR

### Phase 2
- Add peer benchmark fact:
  - industry
  - segment
  - region / unit
  - product cluster
- Replace heuristic whitespace logic with peer-normalized gap logic.

### Phase 3
- Add renewal and transition readiness fields.
- Add optional telemetry / adoption features if source systems are ready.

## Build Order

1. `Executive Product Mix & Industry`
2. `Industry & Business Segment Deep Dive`
3. `Whitespace & Cross-Sell Action Center`
4. `SaaS vs Non-SaaS Transition`
5. Replace the current `Product Portfolio & Whitespace` dashboard once parity is reached

## Known Limits

- Current product family values are commercially useful but not yet a full strategic product hierarchy.
- Current whitespace logic is still heuristic.
- Current delivery-model logic is currently `SaaS` vs `Non-SaaS / Unknown`, not a complete transition model.
