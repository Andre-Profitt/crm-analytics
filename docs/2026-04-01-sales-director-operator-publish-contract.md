# Sales Director Monthly Operator Publish Contract

Date: April 1, 2026

## Purpose

Define which inputs should be treated as live system feeds versus manual operator overlays, and define the minimum publish gate for the monthly Sales Director deck.

## Source Of Truth By Input

### Live CRM / report seams

These should stay automated through the snapshot runner:

- regional target, call, pipeline gap, and confidence reads
- commercial approval summary, approved concentration, and regional miss state
- open renewals due by quarter end, including overdue carryover
- observed churn history from CRM closed-lost renewals
- slipped-deal pressure, repeat pushes, and regional concentration

Why:
- these are already sourced in the snapshot contract and are repeatable month to month
- the value is operational consistency, not operator interpretation

### Manual overlay inputs

These should stay operator-owned until a clean governed source exists:

#### `finance_churn`

- `status`
- `provenance`
- `owner`
- `source_name`
- `headline`
- `summary_note`
- `top_accounts[]`

Decision:
- keep manual for now

Reason:
- the current Finance risk layer is external to the CRM snapshot
- the publish question is not just data presence, but whether Finance has signed off the risk list and owner
- the collection path is now automated through `finance_churn_request.csv`, but the content itself is still operator-owned

#### `slipped_commentary`

- `status`
- `provenance`
- `summary_note`
- `root_cause_bullets[]`
- `owner_comments[]`
- response coverage fields such as `coverage_status`, `requested_item_count`, `provided_comment_count`, and `pending_comment_count`

Decision:
- keep manual for now, but keep the collection process automated

Reason:
- the content is judgment, not a system metric
- the automation value is in building the request pack, owner packets, and response tracking, not in inventing the commentary

## Publish Gate

The deck is publish-ready only when all of the following are true:

1. Snapshot date is correct for the reporting month.
2. Deck rerun completed successfully from the one-command runner.
3. Approval rule contract is aligned to the report target.
4. Renewals scope is aligned to the report target.
5. Renewal and churn methodology remain locked to ACV while land/expand pipeline remains ARR.
6. Finance churn overlay is `provided` and not sample-only.
7. Slipped commentary is `provided`, not sample-only, and not partial.
8. Quick Look thumbnail exists.
9. Validation bundle exists and passes render, montage, overflow, and font checks.
10. PowerPoint-first review bundle is generated, and manual PowerPoint review is completed when Quick Look differs from PowerPoint or when the run introduces material layout changes.

## Current Status

As of the current March 31, 2026 baseline:

- live CRM seams are in place
- commentary collection automation is in place
- Finance churn request-pack automation is now in place
- Finance CSV merge automation is now in place
- publish is still blocked by missing Finance churn input
- publish is still blocked by missing slipped-deal owner commentary
- PowerPoint-first PDF export remains an automated review aid, but it is still session-sensitive and not a replacement for manual PowerPoint signoff when fidelity matters

## Operator Rule

Do not block the workflow waiting for perfect automation of the overlay lanes.

The correct operating model is:
- automated CRM snapshot
- automated `.pptx` generation
- automated validation bundle
- best-effort automated PowerPoint-first review bundle
- manual Finance request send-out with an automated Finance churn request pack
- automated Finance CSV merge once the filled file comes back
- manual Finance risk attachment
- manual owner commentary collection with automated request/packet support
