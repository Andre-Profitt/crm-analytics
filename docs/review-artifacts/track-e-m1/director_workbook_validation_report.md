# Director workbook validation report

- workbook: `<workbook-anchor>`
- contract: `<sanitized>/crm-analytics/config/director_workbook_contract.yaml`
- validated_at: 2026-04-27T02:25:02.301919+00:00
- **status: pass**
- blockers: 0 | warnings: 0
- sheets: 13/13 present
- snapshot_roles: 9 declared

## Resolved snapshot roles

| Role | Source | Sheet | Resolved column | Date | Status |
| --- | --- | --- | --- | --- | --- |
| `q1_opening` | pattern | Q1 Snapshot Trend | `ARR 2026-01-01` | 2026-01-01 | pass |
| `q1_latest` | pattern | Q1 Snapshot Trend | `ARR 2026-04-12` | 2026-04-12 | pass |
| `q1_change_to_close` | pattern | Q1 Snapshot Trend | `ARR Change 2026-04-12` | 2026-04-12 | pass |
| `q1_stage_opening` | pattern | Q1 Snapshot Trend | `StageName_ 2026-01-01` | 2026-01-01 | pass |
| `q1_stage_latest` | pattern | Q1 Snapshot Trend | `StageName_ 2026-04-12` | 2026-04-12 | pass |
| `q2_opening` | pattern | Q2 Snapshot Trend | `ARR 2026-04-01` | 2026-04-01 | pass |
| `q2_latest` | pattern | Q2 Snapshot Trend | `ARR 2026-04-15` | 2026-04-15 | pass |
| `movement_prior` | runtime | — | `—` | — | pass |
| `movement_current` | runtime | — | `—` | — | pass |

## Sheets

| Sheet | Status | Rows | Cols | Missing columns |
| --- | --- | ---: | ---: | --- |
| Summary | pass | 26 | 3 | — |
| Pipeline Open FY26 | pass | 106 | 22 | — |
| Won Lost FY26 | pass | 63 | 12 | — |
| Commercial Approval | pass | 9 | 10 | — |
| Renewals FY26 | pass | 4 | 8 | — |
| Pipeline Inspection | pass | 51 | 9 | — |
| Activity Volume | pass | 106 | 8 | — |
| Commit Items | pass | 61 | 9 | — |
| Q1 Movement | pass | 65 | 9 | — |
| Stage History | pass | 689 | 8 | — |
| Forecast Category History | pass | 496 | 8 | — |
| Q1 Snapshot Trend | pass | 60 | 19 | — |
| Q2 Snapshot Trend | pass | 6 | 19 | — |

