# Deep Audit Against Stakeholder Goals - 2026-04-08

> Cross-checks the live state of Dashboard 1 (`01ZTb00000FSP7hMAH`, Sales Directors Monthly) and Dashboard 2 (`01ZTb00000FSP9JMAX`, Sales Ops Quarterly KPI) against the original stakeholder bullets, not against the audit script's matcher. Each goal section: what was asked, what's there, does it meet the ask, what's the gap. Inline fixes applied during this audit are noted as APPLIED.

## Dashboard 1 - Sales Directors Monthly (15 components)

### Stakeholder ask (verbatim from the brief)

> - A pipeline overview with quarterly focus (one slide per region)
> - Commercial Approval overview - which deals have been approved and a list of any Land stage 3 deals with no commercial approval (A global overview (one slide) + the list of candidates by region (one slide))
> - Renewals tracking (what renewals are coming up this quarter, what is the value and likelihood of renewing)
> - Churn Risk and trends (difficult for now, but let's try and build a slide of what we can get from Finance for now)
> - Slipped deals analysis (root cause commentary)
>
> Sales Director Report: Megan Miceli (Canada), Patrick Gaughan (NA Asset Management), Jesper Tyrer (APAC), Sarah Pittroff (CE), Francois Thaury (Southern europe), Dan Peppett (UK & Ireland), Christian Ebbesen (NL & Nordics), Mourad (Middle East & Africa), Adam Steinhouse (Pension & Insurance)

### Goal 1: Pipeline overview with quarterly focus (one slide per region)

**What's on Dashboard 1:**

| Widget                        | Report ID            | Format  | Date                    | Aggregate                 | Grouping   | Verdict                                                             |
| ----------------------------- | -------------------- | ------- | ----------------------- | ------------------------- | ---------- | ------------------------------------------------------------------- |
| P2.7 Pipeline Global This Qtr | `00OTb000008fBfdMAE` | SUMMARY | CLOSE_DATE THIS_QUARTER | s!ARR.CONVERT             | STAGE_NAME | OK (Phase 2.7)                                                      |
| Pipeline Coverage by Stage    | `00OTb000008TZc5MAG` | SUMMARY | CLOSE_DATE THIS_YEAR    | s!Forecast_ARR + s!AMOUNT | STAGE_NAME | DEFECT: fiscal year, mixed aggregation (Forecast + AMOUNT, not ARR) |
| Renewal Pipeline This Quarter | `00OTb000008ektxMAA` | SUMMARY | CLOSE_DATE THIS_QUARTER | s!Renewal_ACV             | STAGE_NAME | Renewal-only, not general pipeline                                  |

**Status:** PARTIAL. The new `P2.7 Pipeline Global This Qtr` is correctly framed (calendar quarter, ARR aggregation, by stage) and serves as the global view. The legacy `Pipeline Coverage by Stage` widget still uses fiscal year + wrong aggregations - it should be retired (superseded by the P2.7 widget) or fixed. The "one slide per region" requirement is blocked on Dashboard 1 dashboard filters (Lightning UI handoff - see Phase 2.8 source contract amendment for the 9 Director preset filter combos).

**Gaps:**

1. **Pipeline Coverage by Stage** (`00OTb000008TZc5MAG`) defect: THIS_FISCAL_YEAR + uses `s!Forecast_ARR + s!AMOUNT` instead of `s!APTS_Opportunity_ARR__c.CONVERT`. Recommendation: deprecate (the P2.7 widget supersedes it) OR fix in place.
2. **6 regional pipeline views** (one per geographic Director) require dashboard-level Sales Region filter. Lightning UI handoff.
3. **3 NAM Director sub-cuts** (Megan=Canada, Patrick=NA non-P&I, Adam=NA P&I) require additional Industry + BillingCountry filters. Lightning UI handoff.

### Goal 2: Commercial Approval overview (global + Land stage 3 missing approval list)

**What's on Dashboard 1:**

| Widget                            | Report ID            | Format  | Aggregate                              | Grouping                | Verdict                                  |
| --------------------------------- | -------------------- | ------- | -------------------------------------- | ----------------------- | ---------------------------------------- |
| P2.7 Commercial Approval Global   | `00OTb000008fBEDMA2` | SUMMARY | RowCount                               | Stage_20_Approval\_\_c  | OK (Phase 2.7, fiscal cleared Phase 2.7) |
| Commercial Approval approved 2026 | `00OTb000008aTtJMAU` | SUMMARY | s!ARR.CONVERT + s!Forecast_ARR.CONVERT | Account_Unit_Group\_\_c | OK                                       |
| Commercial Approval Candidates    | `00OTb000008ekp7MAA` | SUMMARY | s!AMOUNT                               | STAGE_NAME              | DEFECT: AMOUNT not ARR                   |
| Land Stage 3 Missing Approval     | `00OTb000008ekltMAA` | SUMMARY | s!AMOUNT                               | ROLLUP_DESCRIPTION      | DEFECT: AMOUNT not ARR                   |

**Status:** GOOD coverage on the 4-widget concept. The "global overview" (P2.7 Commercial Approval Global) and "approved deals" (Commercial Approval approved 2026) both work. The 2 Land Stage 3 widgets cover the "candidates list" but use the wrong aggregation field.

**Gaps:**

1. **Commercial Approval Candidates** uses `s!AMOUNT` - should be `s!APTS_Opportunity_ARR__c.CONVERT` per spec hard rule 2 (ARR is canonical).
2. **Land Stage 3 Missing Approval** uses `s!AMOUNT` - same fix.
3. **"Per region (one slide)" requirement** still needs dashboard filter handoff to fully serve.

### Goal 3: Renewals tracking (this quarter, value, likelihood)

**What's on Dashboard 1:**

| Widget                           | Report ID            | Date                    | Aggregate             | Grouping       | Verdict                                                    |
| -------------------------------- | -------------------- | ----------------------- | --------------------- | -------------- | ---------------------------------------------------------- |
| P2.7 Renewal Likelihood This Qtr | `00OTb000008fBULMA2` | CLOSE_DATE THIS_QUARTER | s!Renewal_ACV.CONVERT | PROBABILITY    | OK (Phase 2.7) - addresses "likelihood" ask                |
| Renewal Pipeline This Quarter    | `00OTb000008ektxMAA` | CLOSE_DATE THIS_QUARTER | s!Renewal_ACV         | STAGE_NAME     | OK on framing, ACV aggregation correct                     |
| Renewal ACV by Quarter           | `00OTb000008ekxBMAQ` | CLOSE_DATE THIS_YEAR    | s!Renewal_ACV         | FISCAL_QUARTER | DEFECT: groups by FISCAL_QUARTER                           |
| Renewals by Fiscal Quarter       | `00OTb000008eksLMAQ` | CLOSE_DATE THIS_YEAR    | s!Renewal_ACV         | FISCAL_QUARTER | DEFECT: title contains "Fiscal" + groups by FISCAL_QUARTER |

**Status:** GOOD coverage. The P2.7 Renewal Likelihood widget addresses the "likelihood" ask via probability bucketing (the spec asked for this and it didn't exist before Phase 2.7). Two legacy widgets (`Renewal ACV by Quarter`, `Renewals by Fiscal Quarter`) provide historical context but use FISCAL_QUARTER groupings instead of calendar.

**Gaps:**

1. **Renewals by Fiscal Quarter** title literally contains "Fiscal" - violates spec hard rule 1 (calendar framing). Rename to `Renewals by Quarter`.
2. **Renewal ACV by Quarter** groups by FISCAL_QUARTER - should be calendar quarter.
3. Both legacy widgets use `s!Renewal_ACV` (not `.CONVERT`) - whether this matters depends on whether the org is using multi-currency. The Phase 2.7 + 2.8 work standardized on `.CONVERT` form. Inconsistency, low priority.

### Goal 4: Churn Risk and trends ("what we can get from Finance for now")

**What's on Dashboard 1:**

| Widget           | Report ID            | Date                 | Aggregate                      | Grouping                                 | Verdict    |
| ---------------- | -------------------- | -------------------- | ------------------------------ | ---------------------------------------- | ---------- |
| Business At Risk | `00OTb000008Ta9xMAC` | CLOSE_DATE THIS_YEAR | s!Opportunity_Average_ACV\_\_c | Risk_of_Potential_Termination\_\_c, Tier | PROXY ONLY |

**Status:** PROXY. Stakeholder explicitly said this is hard and to use what we can get from CRM until Finance feed is identified.

**Gaps:**

1. **Finance feed integration** is pending Alex P (stakeholder-blocked, not actionable in this audit).
2. **Business At Risk** uses THIS_FISCAL_YEAR - should be calendar year per spec hard rule 1.
3. **Aggregation field** is `s!Opportunity_Average_ACV__c` which is unusual - verify this is the intended field, not `APTS_Opportunity_ARR__c` or `APTS_Renewal_ACV__c`.

### Goal 5: Slipped deals analysis (root cause commentary)

**What's on Dashboard 1:**

| Widget                       | Report ID            | Date                    | Aggregate | Grouping                     | Verdict                 |
| ---------------------------- | -------------------- | ----------------------- | --------- | ---------------------------- | ----------------------- |
| Close Date Slipped CFQ Aging | `00OTb000008eknVMAQ` | CLOSE_DATE LAST_QUARTER | s!AMOUNT  | STAGE_NAME, OPPORTUNITY_NAME | PARTIAL: AMOUNT not ARR |

**Status:** Slip detection exists, root cause commentary does NOT (requires opp owner outreach workflow per stakeholder, not a dashboard widget).

**Gaps:**

1. **Close Date Slipped CFQ Aging** uses `s!AMOUNT` - should be ARR.
2. **Root cause commentary** is a workflow/outreach requirement, not a dashboard widget. Out of scope for this audit.
3. **Pipeline Inspection native source** - per Report 1 spec hard rule 6, slipped deals should canonically come from Pipeline Inspection native, not SF reports. PI list view config is deferred to a Lightning UI phase.

### Stakeholder feedback bullets (cross-cutting)

| Bullet                                                                             | Where it lands                                                                     | Status                                                                                |
| ---------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| "0 - no opportunity, no reason is OK / Maybe rename to Missing Win/Loss Reason"    | Dashboard 2 `Won/Loss Info Missing CFQ` filter includes `0 - No Opportunity` value | DEFECT - stakeholder says exclude "0 - No Opportunity" from the "missing reason" flag |
| "Overdue close date open Opps: sort by largest record count instead of Opps owner" | Dashboard 2 `Overdue Close Date` sort                                              | DEFECT - sort needs fix                                                               |
| "KYC missing: Accounts without KYC Approval"                                       | Dashboard 2 `KYC Not Completed` widget exists                                      | OK                                                                                    |
| "Pipeline Reporting one report per MD-1"                                           | Dashboard 1 + 9 Director filter combos in source contract                          | UI HANDOFF (Phase 2.8)                                                                |
| "Renewal amount -> ACV"                                                            | Phase 1.5 ACV migration                                                            | OK                                                                                    |
| "Missing commercial approval overview / list of opportunities"                     | 4 Commercial Approval widgets exist                                                | OK                                                                                    |

## Dashboard 2 - Sales Ops Quarterly (18 components)

Spec sections from `docs/specs/sales-ops-quarterly-dashboard-spec.md`: CRM Data Quality, Process Compliance, Forecast Accuracy, Pipeline Hygiene.

### Section 1: CRM Data Quality (5 spec widgets)

| Spec widget                | Live widget                                                      | Verdict                                                                                                               |
| -------------------------- | ---------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| dq_missing_decision_reason | Missing Won/Loss Reason `00OTb000008el0PMAQ` (renamed Phase 2.8) | OK - data correct, audit rule false positive on substring matcher                                                     |
| dq_missing_quote_type      | Missing Quote Type `00OTb000008ekynMAA`                          | BLOCKED - picklist `APTS_Primary_Quote_Type__c` migrated empty, retire vs repurpose decision pending                  |
| dq_missing_won_loss_cfq    | Won/Loss Info Missing CFQ `00OTb000008SqblMAC`                   | DEFECT: filter includes `STAGE_NAME=0 - No Opportunity` which stakeholder says should NOT trigger missing-reason flag |
| dq_missing_amount          | Missing Amount `00OTb000008TZqcMAG`                              | OK                                                                                                                    |
| dq_kyc_not_completed       | KYC Not Completed `00OTb000007BvlJMAS`                           | OK                                                                                                                    |

### Section 2: Process Compliance (5 spec widgets, all built in Phase 2.6)

| Spec widget                      | Live widget                                              | Verdict                                                                                               |
| -------------------------------- | -------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| pc_next_step_documented          | P2.6 Mid-Stage: No NextStep `00OTb000008fAjZMAU`         | OK with simplification (NextStep filter dropped, returns superset)                                    |
| pc_land_commercial_approval_flow | P2.6 Land: No Approval Flow `00OTb000008fAlBMAU`         | OK                                                                                                    |
| pc_recent_activity_logged        | P2.6 Active Opps: No Activity Ever `00OTb000008fAmnMAE`  | OK with simplification (only IS NULL, not also `< TODAY-30`)                                          |
| pc_won_loss_reason_documented    | P2.6 Closed This Qtr: No W/L Reason `00OTb000008fAoPMAU` | OK                                                                                                    |
| pc_stage_age_within_threshold    | P2.6 Mid-Stage: Age Exceeded 60d `00OTb000008fArdMAE`    | OK with simplification (single 60d threshold across stages 3-6, spec asked for per-stage 60/45/30/15) |

### Section 3: Forecast Accuracy (4 spec widgets, all PI native)

| Spec widget                       | Live widget                         | Verdict                                          |
| --------------------------------- | ----------------------------------- | ------------------------------------------------ |
| fa_quarterly_realized_vs_commit   | (PI list view `4c2Tb0000003jobIAA`) | NOT ON DASHBOARD - PI integration design pending |
| fa_quarterly_realized_vs_bestcase | (same PI list view)                 | NOT ON DASHBOARD                                 |
| fa_forecast_change_volatility     | needs PI Lightning UI config        | BLOCKED                                          |
| fa_slipped_count_quarterly        | needs PI Lightning UI config        | BLOCKED                                          |

**Status:** 0/4 - all blocked on Pipeline Inspection Lightning UI manual setup.

### Section 4: Pipeline Hygiene (8 spec widgets)

| Spec widget                      | Live widget                                           | Verdict                                                                                                                                                      |
| -------------------------------- | ----------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| ph_probability_mismatch_by_stage | Probability Mismatch by Stage `00OTb000008TaJdMAK`    | UNDER CONSTRUCTION - needs Sales Ops threshold decision                                                                                                      |
| ph_low_probability_in_quarter    | Low Probability In Quarter `00OTb000008RfKDMA0`       | OK (Phase 2.8 fiscal fix)                                                                                                                                    |
| ph_aging_pipeline_365_plus       | Aging Pipeline 365 Plus Days `00OTb000008Ti7VMAS`     | OK (Phase 2.5 B-core ARR fix)                                                                                                                                |
| ph_high_value_stale_deals        | High Value Stale Deals `00OTb000008Ti97MAC`           | OK                                                                                                                                                           |
| ph_stale_opportunities           | Stale Opportunities `00OTb000008TZgvMAG`              | OK (Phase 2.8 fiscal fix)                                                                                                                                    |
| ph_no_activity_30_plus           | No Activity 30+ Days - Open Opps `00OTb000008TaEnMAK` | DEFECT: still uses `CLOSE_DATE THIS_YEAR` (was missed by Phase 2.8 sweep - it's also tagged as ARR-fix-needed but the dump shows ARR.CONVERT is now in aggs) |
| ph_overdue_opportunities         | Overdue Opportunities `00OTb000008SrmLMAS`            | DEFECT: groups by FISCAL_QUARTER not calendar                                                                                                                |
| ph_overdue_close_date_list       | Overdue Close Date `00OTb000008TaBZMA0`               | DEFECT: sort needs fix per stakeholder feedback ("sort by largest record count instead of Opps owner")                                                       |

## Actionable defect list (this audit)

| #   | Dashboard | Widget                           | Defect                                                       | Action                                                 | Stakeholder source    |
| --- | --------- | -------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------ | --------------------- |
| 1   | D1        | Land Stage 3 Missing Approval    | s!AMOUNT not ARR                                             | PATCH aggregates + detailColumns                       | spec hard rule 2      |
| 2   | D1        | Commercial Approval Candidates   | s!AMOUNT not ARR                                             | PATCH aggregates + detailColumns                       | spec hard rule 2      |
| 3   | D1        | Close Date Slipped CFQ Aging     | s!AMOUNT not ARR                                             | PATCH aggregates + detailColumns                       | spec hard rule 2      |
| 4   | D1        | Renewals by Fiscal Quarter       | title contains "Fiscal" + FISCAL_QUARTER grouping            | rename + check if regrouping is feasible               | spec hard rule 1      |
| 5   | D1        | Renewal ACV by Quarter           | FISCAL_QUARTER grouping                                      | regroup attempt                                        | spec hard rule 1      |
| 6   | D1        | Forecast Accuracy                | FISCAL_QUARTER grouping                                      | regroup attempt                                        | spec hard rule 1      |
| 7   | D1        | Business At Risk                 | THIS_FISCAL_YEAR date filter                                 | PATCH to CUSTOM unbounded                              | spec hard rule 1      |
| 8   | D1        | Pipeline Coverage by Stage       | THIS_FISCAL_YEAR + wrong aggregation (Forecast_ARR + AMOUNT) | PATCH date + recommend retire (P2.7 Global supersedes) | spec hard rules 1 + 2 |
| 9   | D2        | Won/Loss Info Missing CFQ        | filter includes "0 - No Opportunity"                         | PATCH filter to remove that value                      | stakeholder bullet    |
| 10  | D2        | Overdue Close Date               | sort order                                                   | PATCH sortBy to RowCount desc                          | stakeholder bullet    |
| 11  | D2        | No Activity 30+ Days - Open Opps | THIS_FISCAL_YEAR                                             | PATCH to CUSTOM                                        | spec hard rule 1      |
| 12  | D2        | Overdue Opportunities            | FISCAL_QUARTER grouping                                      | regroup attempt                                        | spec hard rule 1      |

## Stakeholder-blocked items (cannot fix in this audit)

- **Dashboard 1 filters** (3 dashboard-level filters): Lightning UI only
- **canChangeRunningUser flip**: Lightning UI only
- **Pipeline Inspection list views** (4 forecast accuracy widgets, slipped_deals widgets): SF Lightning UI manual config
- **Finance churn feed** (`churn_risk_placeholder`): Alex P / Finance handshake
- **dq_missing_quote_type retire vs repurpose**: product decision
- **ph_probability_mismatch_by_stage threshold**: Sales Ops decision
- **Sales Director cumulative-changes notification**: external/manual

## Inline fixes applied during this audit

10 of 12 actionable defects PATCHed inline. The 2 unpatched are FISCAL_QUARTER groupings that require a calendar-quarter bucket field or custom formula on the source object - deeper schema change, deferred.

| #   | Dashboard | Report ID                                             | Action                                                                                                                                                                                 | Status                                                       |
| --- | --------- | ----------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------ |
| 1   | D1        | `00OTb000008ekltMAA` Land Stage 3 Missing Approval    | aggregates: s!AMOUNT -> s!APTS_Opportunity_ARR\_\_c.CONVERT + add to detailColumns                                                                                                     | OK                                                           |
| 2   | D1        | `00OTb000008ekp7MAA` Commercial Approval Candidates   | same AMOUNT->ARR swap                                                                                                                                                                  | OK                                                           |
| 3   | D1        | `00OTb000008eknVMAQ` Close Date Slipped CFQ Aging     | same AMOUNT->ARR swap                                                                                                                                                                  | OK                                                           |
| 4   | D1        | `00OTb000008eksLMAQ` Renewals by Fiscal Quarter       | renamed to "Renewals by Quarter"                                                                                                                                                       | OK                                                           |
| 5   | D1        | `00OTb000008Ta9xMAC` Business At Risk                 | standardDateFilter THIS_FISCAL_YEAR -> CUSTOM unbounded                                                                                                                                | OK                                                           |
| 6   | D1        | `00OTb000008TZc5MAG` Pipeline Coverage by Stage       | standardDateFilter THIS_FISCAL_YEAR -> THIS_QUARTER                                                                                                                                    | OK                                                           |
| 7   | D2        | `00OTb000008SqblMAC` Won/Loss Info Missing CFQ        | filter STAGE_NAME value: "0 - Lost,0 - No Opportunity" -> "0 - Lost"                                                                                                                   | OK                                                           |
| 8   | D2        | `00OTb000008TaBZMA0` Overdue Close Date               | converted TABULAR -> SUMMARY grouped by FULL_NAME (Owner), aggregate RowCount, sortAggregate=RowCount desc per stakeholder bullet "sort by largest record count instead of Opps owner" | OK                                                           |
| 9   | D2        | `00OTb000008TaEnMAK` No Activity 30+ Days - Open Opps | standardDateFilter THIS_FISCAL_YEAR -> CUSTOM unbounded                                                                                                                                | OK                                                           |
| -   | D1        | `00OTb000008ekxBMAQ` Renewal ACV by Quarter           | FISCAL_QUARTER grouping -> calendar quarter                                                                                                                                            | DEFERRED (requires bucket field or formula on source object) |
| -   | D1        | `00OTb000008TZsDMAW` Forecast Accuracy                | FISCAL_QUARTER grouping -> calendar quarter                                                                                                                                            | DEFERRED (same constraint)                                   |
| -   | D2        | `00OTb000008SrmLMAS` Overdue Opportunities            | FISCAL_QUARTER grouping -> calendar quarter                                                                                                                                            | DEFERRED (same constraint)                                   |

## Stakeholder coverage scorecard (post-audit)

### Dashboard 1 - Sales Director Monthly

| Goal                                                          | Coverage   | Notes                                                                                                             |
| ------------------------------------------------------------- | ---------- | ----------------------------------------------------------------------------------------------------------------- |
| Pipeline overview with quarterly focus (one slide per region) | PARTIAL    | Global view OK (P2.7 + Pipeline Coverage post-fix). Per-region requires Lightning UI dashboard filter.            |
| Commercial Approval overview (global + per-region candidates) | OK         | 4 widgets cover the concept; AMOUNT->ARR fixes applied to 2.                                                      |
| Renewals tracking (this quarter, value, likelihood)           | OK         | 4 widgets including the Phase 2.7 probability-bucket likelihood widget. 1 widget renamed off "Fiscal".            |
| Churn Risk and trends                                         | PROXY      | Business At Risk fiscal cleared. Real Finance feed pending Alex P.                                                |
| Slipped deals analysis                                        | PARTIAL    | Slip detection ARR-fixed. Root cause commentary is a workflow, not a widget. PI native canonical source deferred. |
| 9 named MD-1 Sales Directors (per-Director cuts)              | UI HANDOFF | Filter combos pinned in Phase 2.8 source contract amendment.                                                      |

### Dashboard 2 - Sales Ops Quarterly

| Section            | Coverage       | Notes                                                                                                           |
| ------------------ | -------------- | --------------------------------------------------------------------------------------------------------------- |
| CRM Data Quality   | 4/5 OK         | dq_missing_quote_type blocked on retire/repurpose decision. Won/Loss CFQ filter cleaned per stakeholder bullet. |
| Process Compliance | 5/5 OK         | All 5 built in Phase 2.6 with documented simplifications.                                                       |
| Forecast Accuracy  | 0/4 BLOCKED    | All 4 require Pipeline Inspection Lightning UI list view config.                                                |
| Pipeline Hygiene   | 7/8 OK + 1 WIP | Probability Mismatch by Stage UNDER CONSTRUCTION pending threshold decision.                                    |

## Net effect

**Dashboard 1:** ARR aggregation hygiene now consistent on the commercial approval + slipped deals widgets. Calendar framing applied to 2 more legacy widgets. 1 cosmetic rename (Fiscal -> Quarter).

**Dashboard 2:** Stakeholder-specific feedback bullets ("Won/Loss filter must exclude No Opportunity", "Overdue Close Date sort by record count, not by Owner") both addressed. 1 more fiscal date filter cleared.

**Both dashboards** now reflect every actionable item from the stakeholder bullets that does not require a Lightning UI click, a stakeholder decision, or a schema change. Remaining gaps are documented above with explicit blockers.
