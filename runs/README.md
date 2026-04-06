# `runs/` — Builder Audit Trail

Per-run JSON summaries written by every KPI dataset builder at the end of its `main()`. Produced by `crm_analytics_runtime.builder_run()`. Part of Builder Modernization 1A.

## Structure

```
runs/
├── README.md                        ← this file (committed)
├── .gitkeep                         ← keeps the dir in git (committed)
└── <Dataset_Name>/                  ← per-dataset subdir
    └── <YYYYMMDDTHHMMSSZ>.json      ← one per run (gitignored)
```

The JSON filename encodes the UTC start time with colons and dashes stripped, Z-suffixed. Example: `runs/Commercial_Rhythm_Control_Tower/20260406T154720Z.json`.

## JSON shape

See `crm_analytics_runtime.RunSummary` for the authoritative dataclass. Every file contains:

- `dataset_name`, `builder_path` — what ran
- `started_at`, `finished_at`, `runtime_s` — when + how long
- `row_count`, `byte_count` — what got uploaded
- `dataset_id`, `dataset_version_id` — what dataset version the upload produced
- `status` — `"ok"`, `"failed"`, or `"running"` (only if the process was killed mid-run)
- `errors` — exception messages + tracebacks, joined. Empty list on success.
- `external_id` — deterministic 18-char sha256 over `dataset_name|started_at`. Used by future spec 1D's Salesforce upsert loader.
- `summary_schema_version` — `1` for this iteration; future specs that change the shape will bump it.
- `host` — hostname of the machine that ran the builder.

## Retention

Per-run JSONs are gitignored. Keep the last 30 days locally; older than that, delete manually:

```bash
find runs -name "*.json" -mtime +30 -delete
```

## Future: spec 1D

Spec 1D (Salesforce ops dashboard) adds a loader script that globs `runs/**/*.json` and uploads each file as a `Builder_Run__c` record via the REST API, using `external_id` as the upsert key. When that lands, this README gets a "How 1D consumes this dir" section.
