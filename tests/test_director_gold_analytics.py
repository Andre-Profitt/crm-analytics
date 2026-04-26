import dataclasses
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from bundle_factory import make_test_bundle
from scripts.monthly_platform.intelligence import (
    as_rows,
    build_gold_analytics_pack,
    close_date_volatility,
)
from scripts.monthly_platform.models import CloseDateEvent, ForecastEvent, StageEvent
from scripts.build_director_gold_analytics import build_batch


def test_gold_pack_surfaces_concentration_and_deck_insights():
    bundle = make_test_bundle()
    pack = build_gold_analytics_pack(bundle)

    assert pack["artifact_type"] == "director_gold_analytics_pack"
    assert pack["summary"]["open_deals"] == 1
    assert pack["analytics"]["pipeline_concentration"]["bands"][0]["top_n"] == 5
    assert pack["deck_ready_insights"]


def test_close_date_volatility_classifies_push_and_pull():
    bundle = make_test_bundle()
    bundle = dataclasses.replace(
        bundle,
        datasets=dataclasses.replace(
            bundle.datasets,
            close_date_events=[
                CloseDateEvent(
                    opportunity_id="006x",
                    opportunity="Big Deal",
                    account="Acme Corp",
                    owner="Jane Smith",
                    current_stage="3 - Engagement",
                    old_value="2026-06-30",
                    new_value="2026-09-30",
                    created_date="2026-04-01",
                    arr_unweighted=500000,
                    is_closed=False,
                ),
                CloseDateEvent(
                    opportunity_id="006x",
                    opportunity="Big Deal",
                    account="Acme Corp",
                    owner="Jane Smith",
                    current_stage="3 - Engagement",
                    old_value="2026-09-30",
                    new_value="2026-08-31",
                    created_date="2026-04-02",
                    arr_unweighted=500000,
                    is_closed=False,
                ),
            ],
        ),
    )

    volatility = close_date_volatility(as_rows(bundle))

    assert volatility["direction_counts"]["pushed_out"] == 1
    assert volatility["direction_counts"]["pulled_in"] == 1
    assert volatility["by_opportunity"][0]["event_count"] == 2


def test_gold_pack_transition_matrices_include_history_events():
    bundle = make_test_bundle()
    bundle = dataclasses.replace(
        bundle,
        datasets=dataclasses.replace(
            bundle.datasets,
            stage_events=[
                StageEvent(
                    opportunity_id="006x",
                    opportunity="Big Deal",
                    account="Acme Corp",
                    owner="Jane Smith",
                    current_stage="3 - Engagement",
                    old_value="2 - Discovery",
                    new_value="3 - Engagement",
                    created_date="2026-04-01",
                    arr_unweighted=500000,
                    is_closed=False,
                    is_won=False,
                )
            ],
            forecast_category_events=[
                ForecastEvent(
                    opportunity_id="006x",
                    opportunity="Big Deal",
                    account="Acme Corp",
                    owner="Jane Smith",
                    current_stage="3 - Engagement",
                    old_value="Pipeline",
                    new_value="Best Case",
                    created_date="2026-04-01",
                    arr_unweighted=500000,
                )
            ],
        ),
    )

    pack = build_gold_analytics_pack(bundle)

    assert pack["analytics"]["stage_transition_matrix"][0] == {
        "from": "2 - Discovery",
        "to": "3 - Engagement",
        "count": 1,
    }
    assert pack["analytics"]["forecast_transition_matrix"][0] == {
        "from": "Pipeline",
        "to": "Best Case",
        "count": 1,
    }


def test_batch_gold_analytics_builds_each_director_manifest(tmp_path):
    bundle_dir = tmp_path / "bundles" / "2026-04-22"
    bundle_dir.mkdir(parents=True)
    output_root = tmp_path / "gold"
    (bundle_dir / "manifest.json").write_text('{"ignored": true}\n', encoding="utf-8")
    (bundle_dir / "director_bundle_manifest.json").write_text(
        '{"ignored": true}\n',
        encoding="utf-8",
    )
    (bundle_dir / "jesper-tyrer.json").write_text(
        make_test_bundle().to_json(),
        encoding="utf-8",
    )
    (bundle_dir / "christian-ebbesen.json").write_text(
        make_test_bundle(
            director="Christian Ebbesen",
            territory="Northern Europe",
            pipeline_arr=250000.0,
        ).to_json(),
        encoding="utf-8",
    )

    result = build_batch(bundle_dir, output_root)

    manifest_path = output_root / "2026-04-22" / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert result["director_count"] == 2
    assert manifest["director_count"] == 2
    assert manifest["totals"]["open_deals"] == 2
    assert manifest["territories"] == ["APAC", "Northern Europe"]
    rollups = {row["region"]: row for row in manifest["regional_rollups"]}
    assert manifest["rollup_basis"] == "director_book_opportunity_rollup"
    assert rollups["APAC"]["director_count"] == 1
    assert rollups["EMEA"]["director_count"] == 1
    assert rollups["EMEA"]["territories"] == ["Northern Europe"]
    assert (
        output_root / "2026-04-22" / "christian-ebbesen" / "gold_analytics.json"
    ).exists()
