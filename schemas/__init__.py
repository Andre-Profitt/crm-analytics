"""Track I — dataframe contracts.

Two-tier validation for the source-backed warehouse tables:

* ``schemas.pandera.<table>`` — Pandera schemas validated at runtime
  against the in-memory pandas/pyarrow DataFrames the warehouse builds
  before they hit Parquet.
* ``schemas.table_schema.<table>.schema.json`` — Frictionless Table
  Schema (a Frictionless Standard JSON schema) declared portably so
  downstream consumers (BI tooling, Excel, future deck contract,
  non-Python services) can validate Parquet artifacts without
  depending on pandas/pandera.

Both tiers describe the same column shapes anchored against the
2026-04-26 v20d ETL spine checkpoint
(``docs/checkpoints/v20d-etl-spine-checkpoint-2026-04-26/``).
"""
