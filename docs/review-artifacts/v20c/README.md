# v20c Review Artifacts

Live evidence from the source-backed monthly pipeline run `live-all-sources-pipeline-open-v20c` (snapshot `2026-04-30`). Copied here from `output/` (which is `.gitignore`-excluded) so the GPT Pro review brief has GitHub-fetchable proof.

| File                                  | What it is                                                                                          |
| ------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `latest.md`                           | Operator-facing run summary (gate roll-up, status, paths).                                          |
| `latest.json`                         | Machine-readable version of `latest.md`.                                                            |
| `pipeline_run_manifest.json`          | Full 24-stage pipeline manifest with timing, inputs, outputs per stage.                             |
| `source_backed_release_packet.json`   | Release packet — what passed, what blocked, what was uploaded.                                      |
| `source_extract_quality_audit.json`   | Track 3 phase 1 audit: 55/55 sources, required-field checks, row-count policies, fallback warnings. |
| `source_backed_deck_manifest.json`    | Slide-by-slide deck build manifest.                                                                 |
| `source_backed_monthly_review.pptx`   | The 6-slide canonical deck (binary; ChatGPT cannot read inline).                                    |
| `source_backed_analyst_workbook.xlsx` | Analyst workbook (binary).                                                                          |
| `thinkcell_source.xlsx`               | think-cell source workbook (binary).                                                                |

The `.pptx` and `.xlsx` files are binary — ChatGPT cannot ingest them directly, but they're here for download/inspection. JSON+MD files are reviewable inline.
