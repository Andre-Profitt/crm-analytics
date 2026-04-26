# Sales Director Deck Grade Standard

Date: 2026-04-11

## Objective

Define what a director-grade monthly sales deck should be in this repo.

This deck is not a generic presentation and not a dashboard export. It is a monthly operating review for named MD-1 leaders that must be:

- fact-driven
- Salesforce-oriented
- repeatable
- useful in a live leadership discussion
- constrained enough that AI fills it rather than redesigning it

## Product Position

The primary product is the **director deck**.

The system may also produce:

- a **global summary deck** with one operating slide per top-level region
- an internal **regional rollup layer** that feeds the global deck

The director deck is the only place where detailed book-level operating action should live by default.

## Internal Ground Truth

The current workbook and snapshot contract already supports a stronger deck than the shell has been using.

Backed today from the workbook snapshot:

- pipeline ARR by all-open, FY26, and Q2 horizons
- Q2 forecast-category mix (`Pipeline`, `Best Case`, `Commit`, `Omitted`)
- commercial approval summary and candidate lists
- renewal ACV and Q2 renewal watchlist
- Q1 won / lost / slipped actuals
- data-quality controls
- rep-performance concentration
- risk-register outliers
- win/loss reasons and competitor patterns

Explicitly requested controls that the deck model must honor:

- `Missing Win/Loss Reason`
- `0 - No Opportunity` with no reason is acceptable
- `Overdue Close Date Open Opps`
- overdue owner summary sorted by largest record count
- `Renewal amount -> ACV`
- missing commercial approval overview plus list of opportunities
- one report per MD-1
- one global summary deck with one slide per region

Known source gaps that must stay qualified:

- KYC missing accounts
- Finance churn inputs
- quota / target coverage
- owner-commentary depth for slip root cause

## External Research Conclusions

The external evidence supports a constrained operating-deck model rather than freeform slide generation.

### 1. One slide should answer one management question

MIT Communication Lab guidance is clear:

- each slide should convey a single point
- the title should be the slide's main takeaway
- audiences understand simple visuals faster than walls of text

This is directly applicable to a sales director deck. A slide that mixes pipeline, approval, and renewals is structurally weak even if the data fits.

Source:

- https://mitcommlab.mit.edu/eecs/commkit/slideshow/

### 2. Dashboards improve decisions when they reduce perceived complexity

An experimental 2024 study in *Information & Management* found that dashboard format, currency, and completeness affected decision quality indirectly by reducing perceived task complexity and improving information satisfaction.

For this repo, that means:

- keep units and horizons explicit
- reduce clutter
- prefer complete, decision-ready views over partial mixed-context views

Source:

- https://www.sciencedirect.com/science/article/pii/S0378720624000934

### 3. Decision-support dashboards should be refined iteratively against executive questions

A 2025 dashboard-refinement study shows that iterative, feedback-driven refinement around explicit executive questions converts exploratory visuals into decision-support tools.

For this system, that means the shell contract should encode:

- the management question
- the allowed visual family
- the action seam
- the anti-patterns

Source:

- https://arxiv.org/abs/2510.27572

### 4. Pipeline reviews are for management action, not passive reporting

Salesforce's own pipeline-management guidance says pipeline management is more than tracking numbers, and managers should drill into CRM reports with reps and plan specific steps for each deal. Salesforce also emphasizes qualifying deals properly so the pipeline is not clogged with junk that damages forecast accuracy.

For this deck, that means:

- every pipeline slide must lead to action
- top opportunity lists must be curated, not dumped
- hygiene and next-step discipline belong in the deck because they affect forecast trust

Source:

- https://www.salesforce.com/ap/hub/sales/tips-for-sales-pipeline-management/

### 5. Forecast categories and omitted treatment must remain explicit

Salesforce's forecasting guide confirms the standard categories `Pipeline`, `Best Case`, `Commit`, `Closed`, and `Omitted`, and states that `Omitted` is not included in forecasts.

For this deck, that means:

- `Omitted` should always be shown separately
- active headline pipeline must not quietly absorb omitted values
- Q2 forecast slides should use forecast categories explicitly

Source:

- https://resources.docs.salesforce.com/latest/latest/en-us/sfdc/pdf/forecasts.pdf

## Director-Grade Slide Rules

These are non-negotiable for the template model.

### Rule 1: One decision per slide

Every slide must answer a single management question.

### Rule 2: Message-title on populated deck

The shell may start neutral, but the populated deck must rewrite titles into takeaways.

Examples:

- `Q2 active pipeline is thin and concentrated in two deals`
- `Approval governance is controlled overall, but 3 stage 3 land deals need intervention`

### Rule 3: Every slide needs an action seam

Each slide must make it obvious what leadership should do next, even if the action is simply:

- hold the line
- intervene on named deals
- escalate source gaps

### Rule 4: Use semantic frames for every number

Every displayed metric must carry:

- measure type: `ARR` or `ACV`
- currency treatment: `EUR converted`
- horizon: `All Open`, `FY26`, `Q2 2026`, `Q1 actual`, etc.
- inclusion rule when relevant: for example, `Omitted excluded from active headline`

### Rule 5: Watchlists are curated, not exported

Watchlist slides should show the minimum rows needed to drive action.

Default guidance:

- 6-8 rows on ranked watchlists
- 3-5 rows on control summaries
- no raw worksheet dumps in the main body

### Rule 6: Known gaps must stay visible

If the source is missing, the slide must say so.

This applies to:

- churn / Finance inputs
- KYC approval feed
- quota-based coverage
- owner-commentary depth for slip root cause

## Approved Visual Families

The template model should constrain each slide to one family.

- `cover`
- `agenda`
- `four-card-kpi-strip`
- `three-panel-summary`
- `forecast-mix-strip`
- `ranked-watchlist-table`
- `watchlist-plus-owner-summary`
- `control-kpi-plus-watchlist`
- `placeholder-status`
- `appendix-notes`

The shell should not allow arbitrary new families per run.

## Required Management Questions By Slide

### Executive Summary

Question:

- What is this director's position this month, what is the main risk, and what is the one leadership action?

### Q1 Promised vs Delivered

Question:

- What did the book deliver in Q1, what was lost, what slipped, and how safe is the promise baseline?

### Quarterly Pipeline and Forecast

Question:

- What does the current quarter actually look like by forecast category, and where is the exposure?

### Quarterly Opportunity Intel and Coverage Proxy

Question:

- Which deals and hygiene issues most affect the quarter, and how concentrated is the execution burden?

### Commercial Approval Overview

Question:

- Is approval governance under control, and how large is the exposure still pending or missing?

### Missing Commercial Approval Candidates

Question:

- Which specific stage 3+ land deals require immediate action?

### Renewals and Retention

Question:

- What renews this quarter, what is the ACV, and where does leadership need to intervene?

### Slipped Deals and Follow-up

Question:

- Which deals slipped, how large is the slip, and what follow-up is required now?

### Salesforce Hygiene and Activity Controls

Question:

- Where is CRM discipline weak enough to threaten forecast trust or execution quality?

### Missing Win/Loss Reason

Question:

- Which outcome records still lack decision-reason hygiene, after applying the `0 - No Opportunity` exception?

### Overdue Close Date Open Opportunities

Question:

- Which open opportunities are past close date, and where is overdue-close burden concentrated?

### Churn Risk and Finance Inputs

Question:

- What Finance-owned churn input exists, what is missing, and who owns the feed?

### Appendix and Factual Notes

Question:

- What definitions and caveats does leadership need to interpret the deck correctly?

## Specific Model Changes Required In This Repo

1. Add presentation-grade fields to the shell contract:
   - `management_question`
   - `visual_family`
   - `action_seam`
   - `title_rewrite_rule`
   - `density_limit`
   - `anti_patterns`
2. Validate those fields as part of the contract gate.
3. Carry those fields into the structured fill payload sent to PowerPoint.
4. Downgrade slide titles that overclaim unsupported data.
   - Example: use `Slipped Deals and Follow-up` instead of promising deep root-cause commentary before owner input exists.
5. Keep `coverage` explicitly qualified until quota / target sources are integrated.

## Publish Gate Implications

A director deck is not publishable if any of these are true:

- unlabeled pipeline numbers appear without ARR/ACV and horizon
- `Omitted` is buried inside active pipeline
- a slide mixes multiple management questions
- watchlists are dumped raw from Salesforce
- root-cause language exceeds the source evidence
- missing-source placeholders are hidden or fabricated
- shell scaffolding text is still visible

## Next Build Priority

1. Upgrade the director shell contract to include the grade-standard fields.
2. Upgrade the validator to enforce them.
3. Upgrade the structured fill payload and PowerPoint prompt to use them.
4. Then continue the visual refinement of the shell against the same standard.
