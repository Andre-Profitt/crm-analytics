# Deck render report

- pptx: `<pptx-anchor>`
- deck contract: `<sanitized>/crm-analytics/config/deck_contract.yaml`
- validated_at: 2026-04-27T13:14:10.435959+00:00
- **status: fail**
- blockers: 1 | warnings: 13

| # | Slide | Title | Title top | Tables | Footer | Overall |
| ---: | --- | --- | ---: | --- | --- | --- |
| 1 | `cover` | fail | — | n/a | pass | fail |
| 2 | `executive_summary` | pass | 0.295 | n/a | fail | pass |
| 3 | `since_last_review` | pass | 0.295 | pass | fail | pass |
| 4 | `q1_promised_delivered` | pass | 0.295 | n/a | fail | pass |
| 5 | `q1_loss_drivers` | pass | 0.295 | pass | fail | pass |
| 6 | `q2_outlook` | pass | 0.295 | n/a | fail | pass |
| 7 | `top_open_opportunities` | pass | 0.295 | pass | fail | pass |
| 8 | `deal_risk_triage` | pass | 0.295 | pass | fail | pass |
| 9 | `owner_coaching` | pass | 0.295 | pass | fail | pass |
| 10 | `pushed_deals` | pass | 0.295 | n/a | pass | pass |
| 11 | `q1_slippage` | pass | 0.295 | pass | fail | pass |
| 12 | `forecast_accuracy` | pass | 0.295 | n/a | fail | pass |
| 13 | `forecast_mix` | pass | 0.295 | pass | fail | pass |
| 14 | `commercial_approvals` | pass | 0.295 | pass | pass | pass |
| 15 | `renewals` | pass | 0.295 | pass | fail | pass |
| 16 | `legal_notice` | n/a | — | n/a | n/a | pass |

## Findings

- **blocker** `title_placeholder_missing` — slide 1 (cover) has no placeholder 144 with a non-empty title text
- **warning** `footer_missing` — slide 2 (executive_summary) has no text frame in the footer band (top >= 6.5")
- **warning** `footer_missing` — slide 3 (since_last_review) has no text frame in the footer band (top >= 6.5")
- **warning** `footer_missing` — slide 4 (q1_promised_delivered) has no text frame in the footer band (top >= 6.5")
- **warning** `footer_missing` — slide 5 (q1_loss_drivers) has no text frame in the footer band (top >= 6.5")
- **warning** `footer_missing` — slide 6 (q2_outlook) has no text frame in the footer band (top >= 6.5")
- **warning** `footer_missing` — slide 7 (top_open_opportunities) has no text frame in the footer band (top >= 6.5")
- **warning** `footer_missing` — slide 8 (deal_risk_triage) has no text frame in the footer band (top >= 6.5")
- **warning** `footer_missing` — slide 9 (owner_coaching) has no text frame in the footer band (top >= 6.5")
- **warning** `footer_missing` — slide 11 (q1_slippage) has no text frame in the footer band (top >= 6.5")
- **warning** `footer_missing` — slide 12 (forecast_accuracy) has no text frame in the footer band (top >= 6.5")
- **warning** `footer_missing` — slide 13 (forecast_mix) has no text frame in the footer band (top >= 6.5")
- **warning** `footer_missing` — slide 15 (renewals) has no text frame in the footer band (top >= 6.5")
- **warning** `legal_disclaimer_missing` — slide 16 (legal_notice) has no text containing any of ['SimCorp', 'Confidential', 'disclaimer']
