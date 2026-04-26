# Sales Region Monthly Shell Design System

Date: 2026-04-11

## Objective

Build a polished regional leadership deck that is:

- SimCorp-branded
- repeatable every month
- easy for Claude to populate without redesigning the presentation
- strict about factual inputs
- strong enough for regional sales leadership and sales-operations review

This shell is not a freeform presentation template. It is a controlled operating deck.

## Design Position

The right pattern is:

1. Use real SimCorp masters and brand system as the visual base.
2. Use real internal operating-report structure as the narrative base.
3. Use external executive-presentation best practices to constrain slide density and message clarity.
4. Let Claude fill content into a fixed grammar instead of inventing slide structure.

## What We Should Copy From Real-World Examples

### Internal patterns to reuse

Use the stronger visual rhythms already present in:

- `scripts/simcorp_full_deck_v3.py`
- `scripts/build_report1_simcorp_from_snapshot.py`
- the SimCorp master at `~/archive/simcorp-deck-agent-backup/reference-decks/SimCorp_PPT_Template.pptx`

These internal examples are useful because they already show:

- credible SimCorp color use
- executive KPI strips
- hero-stat slides
- horizontal comparison bars
- card-based governance slides
- enough visual variation that the deck does not feel like 13 copies of the same slide

### External principles to copy

We should borrow principles, not visual styling, from external sources:

- recommendation-first communication
- one main idea per slide
- strong slide titles that state the point
- limited text per slide
- repeatable operating cadence
- pipeline reviews focused on action and forecast trust, not history recital

## Non-Negotiable Slide Rules

### Rule 1: One decision per slide

Every slide must answer a single leadership question.

Bad:

- a slide that mixes pipeline, churn, and renewals because the data fits

Good:

- one slide answers whether quarterly pipeline is enough
- one slide answers whether approvals are under control
- one slide answers where renewals need intervention

### Rule 2: Recommendation-first titles

The visible slide title should become a message, not just a topic label, once populated.

Examples:

- `EMEA Q2 active pipeline is thin versus target and concentrated in two books`
- `Commercial approval exposure is contained overall but 5 land stage 3 deals need action`
- `Renewal risk is concentrated in overdue carryover, not in the total book`

The shell can start with neutral titles, but the populated deck should rewrite them into takeaway titles.

### Rule 3: Every slide needs an action seam

A leadership deck is not a dashboard export.

Every substantive slide should answer one of:

- what is happening
- why it matters
- what leadership should do next

### Rule 4: Placeholder text must read like editorial guidance, not scaffolding

Bad placeholder:

- `Validated EUR ARR`
- `Insert metric`

Better placeholder:

- `Populate with validated Q2 active ARR in EUR; keep horizon explicit`
- `State the single leadership action this region needs this month`

The current shell is structurally stronger now, but some placeholders still read like system fields. Those need another pass.

### Rule 5: Variation must be systematic

We do want visual variation, but controlled variation.

Approved slide families:

- cover
- agenda
- KPI strip
- hero + stacked mini-stats
- 3-column card comparison
- structured watchlist table
- side-by-side summary panels
- appendix notes

Do not let Claude invent new families ad hoc.

### Rule 6: Numbers never sit on a slide without a semantic frame

Each metric shown needs:

- measure type: ARR or ACV
- currency treatment: EUR converted
- horizon: all open, FY26, Q2, Q1 actual, etc.
- inclusion rule if relevant: for example, omitted excluded from active pipeline

## Sales / Sales Ops Specific Content Rules

### Pipeline slide

Should answer:

- is the quarter adequately covered
- how much is active versus commit versus best case
- what is excluded as omitted
- whether the region is concentrated or thin

Should not:

- dump a generic opportunity list
- present a single unlabeled pipeline number

### Q1 promised vs delivered

Should answer:

- what we actually won
- what we lost
- what slipped
- whether the “promise” baseline is forecast-safe or ambiguous

Should not:

- overstate commitment precision
- use contaminated workbook tab logic if the baseline is not director-safe or region-safe

### Commercial approval

Should answer:

- whether governance is under control
- how many stage 3+ deals are approved
- which deals still need action

Should not:

- treat pending and missing as the same thing without explanation
- bury the candidate list in appendix-style text

### Renewals

Should answer:

- what renews this quarter
- value in EUR-converted ACV
- risk distribution
- which named renewals need leadership action

Should not:

- mix renewal ACV with new-business ARR

### Slipped deals

Should answer:

- how much slipped
- where it sits
- what follow-up is required

Should not:

- pretend the root cause is known if owner commentary does not exist yet

### Churn

Should answer:

- what Finance input exists
- what is missing
- who owns the reporting path

Should not:

- fabricate churn trend numbers

## Proposed Build Method

### Phase 1: Lock the shell grammar

For each slide, define:

- slide purpose
- allowed visual family
- required slots
- allowable title rewrite behavior
- maximum placeholder density

This should live in config, not just in code comments.

### Phase 2: Tighten shell placeholders

Replace remaining generic placeholders with editorial prompts.

Examples:

- replace `Validated EUR ARR` with `Q2 active ARR in EUR (validated fact pack)`
- replace `Summary statement` with `State whether coverage is healthy, thin, or concentrated`

### Phase 3: Build one “golden populated example”

Use EMEA as the reference populated deck.

That gives us:

- one high-quality visual target
- one test case for future shell regressions
- one benchmark for Claude output quality

### Phase 4: Make Claude fill only within bounds

Claude PowerPoint should be instructed to:

- preserve layouts
- replace shell guidance with validated content
- rewrite titles into takeaway titles
- avoid adding new shapes unless necessary

Claude should not:

- redesign the slide family
- move sections around
- invent new metrics

## Immediate Refinement Priorities

1. Tighten placeholder language on executive summary, Q1, pipeline, and appendix slides.
2. Make visible slide titles more recommendation-ready once populated.
3. Build one gold-standard populated EMEA regional deck.
4. Use that populated EMEA deck as the visual benchmark for APAC and North America.

## Acceptance Criteria

The regional shell is good enough when:

- it looks credible before population
- it looks polished after replacing placeholders with facts
- every slide has a clear leadership question
- no slide feels like a worksheet export
- no slide feels like a random consulting-template clone
- PowerPoint Claude can fill it without distorting structure

## Source Notes

External best-practice inputs used for this design system:

- MIT Communication Lab on slide design and message-first titles
- MITAA PowerPoint best practices on one main idea per slide and density control
- Salesforce pipeline review guidance on action-oriented review cadence
- Gong operating rhythm guidance on separating pipeline review from broader forecast rollups
- Barbara Minto / Pyramid Principle as the communication model for executive decks
