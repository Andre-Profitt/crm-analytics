# Fact Check Order

## 1. Snapshot First

Use the normalized director snapshot as the first read:

- `scorecard`
- `q2_outlook`
- `commercial_approval`
- `renewals`
- `q1_review`
- `risk_register`

## 2. Fact Pack Second

If a validated fact pack exists, compare the claim to:

- `validated-fact-pack.md`
- `validation-report.json`

## 3. Raw Claude Last

Excel-Claude and PowerPoint-Claude output are useful, but they are not authoritative until checked.

## 4. Common Failure Modes

- unlabeled pipeline number that mixes all-open and quarter views
- renewal value shown as ARR instead of ACV
- Q1 promise baseline taken from global workbook blocks
- omitted-stage values treated as active pipeline
- deck text lagging the corrected workbook
