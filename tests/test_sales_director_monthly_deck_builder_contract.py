from __future__ import annotations

import json
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[1]
BUILDER = ROOT / "output" / "sales_director_monthly_deck_2026-03-31" / "build_sales_director_monthly_deck.js"
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "sales_director_monthly"


def run_builder_eval(script_body: str) -> dict:
    script = f"""
const fs = require("fs");
const builder = require({json.dumps(str(BUILDER))});
{script_body}
"""
    result = subprocess.run(
        ["node", "-e", script],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr or result.stdout
    return json.loads(result.stdout)


def fixture_path(name: str) -> str:
    return str((FIXTURE_DIR / f"{name}.json").resolve())


def test_short_title_truncates_on_word_boundary() -> None:
    payload = run_builder_eval(
        """
const value = builder.shortTitle(
  "Observed churn is clear and forward Finance risk is still sample only for this month",
  38
);
console.log(JSON.stringify({ value }));
"""
    )

    assert len(payload["value"]) <= 38
    assert not payload["value"].endswith(" and…")
    assert not payload["value"].endswith(" for…")


def test_dynamic_titles_stay_within_expected_limits() -> None:
    payload = run_builder_eval(
        f"""
const high = JSON.parse(fs.readFileSync({json.dumps(fixture_path("high_risk_snapshot"))}, "utf8"));
const missing = JSON.parse(fs.readFileSync({json.dumps(fixture_path("missing_overlay_snapshot"))}, "utf8"));
const biggestSlip = Object.entries(high.slipped_deals.summary_by_region).sort((a, b) => b[1].slipped_arr - a[1].slipped_arr)[0];
const result = {{
  region: builder.regionSlideTitle("North America", high.pipeline.deck_regions["North America"]),
  approval: builder.approvalSlideTitle(high.commercial_approval),
  renewals: builder.renewalsSlideTitle(high.renewals),
  churn: builder.churnSlideTitle(missing.external_inputs.finance_churn),
  slipped: builder.slippedSlideTitle(biggestSlip)
}};
console.log(JSON.stringify(result));
"""
    )

    assert len(payload["region"]) <= 48
    assert len(payload["approval"]) <= 44
    assert len(payload["renewals"]) <= 52
    assert len(payload["churn"]) <= 54
    assert len(payload["slipped"]) <= 48


def test_kicker_language_is_standardized() -> None:
    payload = run_builder_eval(
        f"""
const high = JSON.parse(fs.readFileSync({json.dumps(fixture_path("high_risk_snapshot"))}, "utf8"));
const missing = JSON.parse(fs.readFileSync({json.dumps(fixture_path("missing_overlay_snapshot"))}, "utf8"));
const biggestSlip = Object.entries(high.slipped_deals.summary_by_region).sort((a, b) => b[1].slipped_arr - a[1].slipped_arr)[0];
const result = {{
  region: builder.regionSlideKicker(high.pipeline.deck_regions["North America"]),
  approval: builder.approvalSlideKicker(high.commercial_approval),
  renewals: builder.renewalsSlideKicker(high.renewals),
  churn: builder.churnSlideKicker(missing.external_inputs.finance_churn),
  slipped: builder.slippedSlideKicker(biggestSlip ? missing.external_inputs.slipped_commentary : {{}})
}};
console.log(JSON.stringify(result));
"""
    )

    assert payload["region"] == "Regional outlook | coverage creation"
    assert payload["approval"].startswith("Commercial governance |")
    assert payload["renewals"].startswith("Retention risk |")
    assert payload["churn"] == "Churn risk | forward-risk gap"
    assert payload["slipped"] == "Slipped pipeline | repeat-push review"


def test_management_bullets_stay_concise_for_missing_overlay_case() -> None:
    payload = run_builder_eval(
        f"""
const missing = JSON.parse(fs.readFileSync({json.dumps(fixture_path("missing_overlay_snapshot"))}, "utf8"));
const biggestSlip = Object.entries(missing.slipped_deals.summary_by_region).sort((a, b) => b[1].slipped_arr - a[1].slipped_arr)[0];
const result = {{
  churnBullets: builder.managementBullets("churn", {{
    financeOverlay: missing.external_inputs.finance_churn,
    churn: missing.churn
  }}),
  slippedBullets: builder.managementBullets("slipped", {{
    biggestSlip,
    slippedOverlay: missing.external_inputs.slipped_commentary,
    slippedDeals: missing.slipped_deals
  }})
}};
console.log(JSON.stringify(result));
"""
    )

    assert 1 <= len(payload["churnBullets"]) <= 3
    assert 1 <= len(payload["slippedBullets"]) <= 3
    assert all(len(item) <= 92 for item in payload["churnBullets"])
    assert all(len(item) <= 92 for item in payload["slippedBullets"])


def test_executive_read_helpers_return_expected_card_shapes() -> None:
    payload = run_builder_eval(
        f"""
const missing = JSON.parse(fs.readFileSync({json.dumps(fixture_path("missing_overlay_snapshot"))}, "utf8"));
const high = JSON.parse(fs.readFileSync({json.dumps(fixture_path("high_risk_snapshot"))}, "utf8"));
const result = {{
  churnLowerTitle: builder.tableTitle("churn", "lower", {{ hasPublishableFinanceOverlay: false }}),
  slippedLowerTitle: builder.tableTitle("slipped", "lower", {{ hasOwnerComments: false }}),
  renewalLowerTitle: builder.tableTitle("renewals", "actions"),
  churnCards: builder.churnForwardActionCards(missing.external_inputs.finance_churn, missing.churn),
  renewalCards: builder.renewalFocusCards(high.renewals.top_open_renewals.slice(0, 3)),
  slippedCards: builder.slippedPriorityCards(high.slipped_deals)
}};
console.log(JSON.stringify(result));
"""
    )

    assert payload["churnLowerTitle"] == "Forward-risk wiring before publish"
    assert payload["slippedLowerTitle"] == "Recovery proof points"
    assert payload["renewalLowerTitle"] == "Quarter-critical renewal proof points"
    assert len(payload["churnCards"]) == 3
    assert payload["churnCards"][0]["value"] == "Blocked"
    assert len(payload["renewalCards"]) == 3
    assert all("ACV" in item["value"] for item in payload["renewalCards"])
    assert len(payload["slippedCards"]) == 3
    assert "pushes" in payload["slippedCards"][0]["signal"]


def test_missing_finance_churn_uses_proof_point_panel_contract() -> None:
    payload = run_builder_eval(
        f"""
const missing = JSON.parse(fs.readFileSync({json.dumps(fixture_path("missing_overlay_snapshot"))}, "utf8"));
const result = {{
  sideTitle: builder.tableTitle("churn", "side", {{ hasFinanceAccounts: false }}),
  proofCards: builder.churnObservedProofCards(missing.churn)
}};
console.log(JSON.stringify(result));
"""
    )

    assert payload["sideTitle"] == "Observed churn proof points"
    assert len(payload["proofCards"]) == 3
    assert payload["proofCards"][0]["title"] == "Top owner share"
    assert "deals" in payload["proofCards"][0]["value"]
    assert any(card["title"] == "Peak quarter" for card in payload["proofCards"])


def test_missing_finance_churn_implication_rows_are_structured() -> None:
    payload = run_builder_eval(
        f"""
const missing = JSON.parse(fs.readFileSync({json.dumps(fixture_path("missing_overlay_snapshot"))}, "utf8"));
const result = builder.churnImplicationRows(
  missing.external_inputs.finance_churn,
  missing.churn
);
console.log(JSON.stringify(result));
"""
    )

    assert len(payload) == 3
    assert [row["label"] for row in payload] == ["Read", "Anchor", "Gate"]
    assert payload[0]["text"].startswith("This page is still historical CRM churn")
    assert "ACV" in payload[1]["text"]
    assert "Finance risk list" in payload[2]["text"]


def test_missing_commentary_slipped_uses_recovery_proof_cards() -> None:
    payload = run_builder_eval(
        f"""
const missing = JSON.parse(fs.readFileSync({json.dumps(fixture_path("missing_overlay_snapshot"))}, "utf8"));
const biggestSlip = Object.entries(missing.slipped_deals.summary_by_region).sort((a, b) => b[1].slipped_arr - a[1].slipped_arr)[0];
const result = builder.slippedRecoveryProofCards(
  biggestSlip,
  missing.external_inputs.slipped_commentary,
  missing.slipped_deals
);
console.log(JSON.stringify(result));
"""
    )

    assert len(payload) == 3
    assert payload[0]["title"] == "Late-stage slippage"
    assert "Commit or Contracting" in payload[0]["signal"]
    assert payload[1]["title"] == "Highest-value repeat push"
    assert "pushes" in payload[1]["value"]
    assert payload[2]["title"] == "Commentary gate"
    assert payload[2]["value"] == "Pending"


def test_missing_commentary_slipped_implication_rows_are_structured() -> None:
    payload = run_builder_eval(
        f"""
const missing = JSON.parse(fs.readFileSync({json.dumps(fixture_path("missing_overlay_snapshot"))}, "utf8"));
const biggestSlip = Object.entries(missing.slipped_deals.summary_by_region).sort((a, b) => b[1].slipped_arr - a[1].slipped_arr)[0];
const result = builder.slippedImplicationRows(
  biggestSlip,
  missing.external_inputs.slipped_commentary,
  missing.slipped_deals
);
console.log(JSON.stringify(result));
"""
    )

    assert len(payload) == 3
    assert [row["label"] for row in payload] == ["Read", "Anchor", "Gate"]
    assert "anchors the current-quarter slipped ARR queue" in payload[0]["text"]
    assert "Commit or Contracting" in payload[1]["text"] or "highest-value repeat push" in payload[1]["text"]
    assert "quantified recovery pressure" in payload[2]["text"]


def test_slipped_repeat_push_rows_use_bar_list_contract() -> None:
    payload = run_builder_eval(
        f"""
const missing = JSON.parse(fs.readFileSync({json.dumps(fixture_path("missing_overlay_snapshot"))}, "utf8"));
const result = {{
  repeatTitle: builder.tableTitle("slipped", "repeat"),
  rows: builder.slippedRepeatPushBarRows(missing.slipped_deals)
}};
console.log(JSON.stringify(result));
"""
    )

    assert payload["repeatTitle"] == "Repeat-push ARR anchors"
    assert 1 <= len(payload["rows"]) <= 5
    assert payload["rows"][0]["label"].endswith("4x")
    assert payload["rows"][0]["value"] > payload["rows"][-1]["value"]


def test_cover_readout_rows_stay_short_and_structured() -> None:
    payload = run_builder_eval(
        f"""
const missing = JSON.parse(fs.readFileSync({json.dumps(fixture_path("missing_overlay_snapshot"))}, "utf8"));
const regions = missing.pipeline.deck_regions;
const biggestGap = Object.entries(regions).sort((a, b) => b[1].needed_from_pipeline_arr - a[1].needed_from_pipeline_arr)[0];
const biggestSlip = Object.entries(missing.slipped_deals.summary_by_region).sort((a, b) => b[1].slipped_arr - a[1].slipped_arr)[0];
const result = builder.coverReadoutRows(
  biggestGap,
  missing.renewals,
  biggestSlip,
  missing.external_inputs.finance_churn,
  missing.external_inputs.slipped_commentary
);
console.log(JSON.stringify(result));
"""
    )

    assert len(payload) == 3
    assert [row["label"] for row in payload] == ["Coverage", "Renewals", "Pressure"]
    assert all(len(row["text"]) <= 90 for row in payload)
    assert payload[2]["text"].endswith("pending.")


def test_regression_fixtures_cover_low_high_and_missing_overlay_states() -> None:
    payload = run_builder_eval(
        f"""
function config(snapshot) {{
  return {{
    snapshot,
    outputPath: "/tmp/deck.pptx",
    summaryPath: "/tmp/deck.summary.json",
    snapshotPath: "/tmp/snapshot.json"
  }};
}}
const low = JSON.parse(fs.readFileSync({json.dumps(fixture_path("low_risk_snapshot"))}, "utf8"));
const high = JSON.parse(fs.readFileSync({json.dumps(fixture_path("high_risk_snapshot"))}, "utf8"));
const missing = JSON.parse(fs.readFileSync({json.dumps(fixture_path("missing_overlay_snapshot"))}, "utf8"));
const result = {{
  low: builder.buildSummary(config(low)),
  high: builder.buildSummary(config(high)),
  missing: builder.buildSummary(config(missing))
}};
console.log(JSON.stringify(result));
"""
    )

    assert payload["low"]["publish_status"] == "final_qa_ready"
    assert payload["low"]["publish_blockers"] == []
    assert payload["high"]["publish_status"] == "final_qa_ready"
    assert payload["high"]["approval_candidate_count"] == 4
    assert payload["missing"]["publish_status"] == "internal_review_ready"
    assert payload["missing"]["publish_blockers"] == [
        "Finance churn input is still missing",
        "Slipped-deal owner commentary is still missing",
    ]
