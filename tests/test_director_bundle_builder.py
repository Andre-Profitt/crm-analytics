from pathlib import Path

import json

from scripts.monthly_platform.director_bundle_builder import (
    build_director_bundle_from_source_bundle,
    build_director_bundles_from_source_bundles,
)
from scripts.monthly_platform.director_bundle_contract import (
    load_director_bundle_contract,
)
from scripts.monthly_platform.models import DirectorBundle
from scripts.monthly_platform.source_bundles import (
    HistoricalTrendRows,
    PipelineDisplayDecision,
    PipelineInspectionRow,
    PipelineOpenRow,
    SourceBundleManifest,
    TerritorySourceBundle,
)


def _source_bundle() -> TerritorySourceBundle:
    return TerritorySourceBundle(
        snapshot_date="2026-04-30",
        source_run_id="source-run",
        generated_at="2026-04-30T09:30:00Z",
        territory="APAC",
        director="Jesper Tyrer",
        period_context={},
        source_extract_ids=["src-current", "src-forward", "src-history"],
        pipeline_open=[
            PipelineOpenRow(
                opportunity="Big Deal",
                account="Acme",
                owner="Owner",
                stage="3 - Engagement",
                forecast_category="Commit",
                close_date="2026-06-30",
                arr_unweighted=500.0,
                arr_weighted=250.0,
                probability=50.0,
                deal_type="Land",
                sales_region="APAC",
                created_date="2026-01-01",
                last_modified_date="2026-04-01",
                currency="EUR",
                quarter="Q2 2026",
                source_extract_id="src-pipeline-open",
            )
        ],
        historical_trending=HistoricalTrendRows(
            prior_quarter=[
                {
                    "Opportunity Name": "Big Deal",
                    "Account Name": "Acme",
                    "Close Date": "2026-06-30",
                    "Forecast ARR (converted) (2026-04-01)": "EUR 100,00",
                    "Forecast ARR (converted) (2026-04-07)": "EUR 250,00",
                    "Stage (2026-04-01)": "2 - Discovery",
                    "Stage (2026-04-07)": "3 - Engagement",
                }
            ]
        ),
        pi_current=[
            PipelineInspectionRow(
                opportunity="Big Deal",
                account="Acme",
                owner="Owner",
                stage="3 - Engagement",
                forecast_category="Commit",
                arr_weighted=250.0,
                currency="EUR",
                close_date="2026-06-30",
                push_count=1,
                score=75,
                priority=True,
                source_extract_id="src-current",
                active_in_period=True,
            )
        ],
        pi_forward=[
            PipelineInspectionRow(
                opportunity="Forward Deal",
                owner="Owner",
                stage="2 - Discovery",
                forecast_category="Pipeline",
                arr_weighted=100.0,
                currency="EUR",
                close_date="2026-09-30",
                source_extract_id="src-forward",
                active_in_period=True,
            )
        ],
        pipeline_display_decision=PipelineDisplayDecision(
            display_period_role="current_quarter",
            display_quarter_label="Q2",
            display_quarter_title="Q2 2026",
            display_reason="current_quarter",
            requires_forward_quarter_fallback=False,
            current_quarter_active_deals=1,
            current_quarter_active_arr=250.0,
            forward_quarter_active_deals=1,
            forward_quarter_active_arr=100.0,
        ),
    )


def test_build_director_bundle_maps_source_backed_datasets() -> None:
    bundle = build_director_bundle_from_source_bundle(_source_bundle())

    assert bundle.director == "Jesper Tyrer"
    assert bundle.territory == "APAC"
    assert bundle.dataset_counts["pipeline_open"] == 1
    assert bundle.dataset_counts["pi_current"] == 1
    assert bundle.dataset_counts["pi_forward"] == 1
    assert bundle.dataset_counts["snapshot_trend"] == 2
    assert bundle.datasets.pi_current[0].opportunity == "Big Deal"
    assert bundle.datasets.pipeline_open[0].arr_unweighted == 500.0
    assert bundle.datasets.pi_current[0].arr_weighted == 250.0
    assert bundle.datasets.snapshot_trend[1].arr_at_snapshot == 250.0


def test_director_bundle_round_trips_through_legacy_model() -> None:
    bundle = build_director_bundle_from_source_bundle(_source_bundle())
    restored = DirectorBundle.from_json(bundle.to_json())

    assert restored == bundle


def test_build_director_bundles_from_source_bundles_writes_manifest(
    tmp_path: Path,
) -> None:
    source_dir = tmp_path / "source-bundles"
    source_dir.mkdir()
    source_bundle_path = source_dir / "apac.json"
    source_bundle_path.write_text(
        _source_bundle().model_dump_json(indent=2) + "\n",
        encoding="utf-8",
    )
    source_manifest = SourceBundleManifest(
        snapshot_date="2026-04-30",
        source_run_id="source-run",
        status="ok",
        source_manifest_path="source-run/run_manifest.json",
        output_dir=str(source_dir),
        territory_count=1,
        bundle_paths=[str(source_bundle_path)],
        summary={},
    )
    (source_dir / "source_bundle_manifest.json").write_text(
        source_manifest.model_dump_json(indent=2) + "\n",
        encoding="utf-8",
    )

    manifest = build_director_bundles_from_source_bundles(
        source_bundle_dir=source_dir,
        output_dir=tmp_path / "director-bundles",
        require_valid=True,
        bundle_contract=load_director_bundle_contract(
            Path("config/monthly_director_bundle_contract.json")
        ),
    )

    assert manifest.status == "ok"
    assert manifest.summary["bundle_count"] == 1
    assert (
        manifest.summary["directors"][0]["dataset_coverage"]["source_backed"]
        == ["pi_current", "pi_forward", "pipeline_open", "snapshot_trend"]
    )
    output_bundle = json.loads(Path(manifest.bundle_paths[0]).read_text())
    assert output_bundle["dataset_counts"]["pipeline_open"] == 1
    assert output_bundle["dataset_counts"]["pi_current"] == 1
    assert (
        output_bundle["source_contract"]["sources"]["source_bundle"]["source_id"]
        == "source-run"
    )
