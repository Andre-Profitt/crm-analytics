PYTHON ?= python3

ROOT := $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
CORE_FILES := \
	$(ROOT)/crm_analytics_helpers.py \
	$(ROOT)/portfolio_foundation.py \
	$(ROOT)/build_dashboard.py \
	$(ROOT)/build_revenue_motions.py \
	$(ROOT)/build_sales_compliance.py \
	$(ROOT)/build_customer_intelligence.py \
	$(ROOT)/build_account_intelligence.py \
	$(ROOT)/build_lead_management.py \
	$(ROOT)/build_contract_operations.py \
	$(ROOT)/build_forecasting.py \
	$(ROOT)/build_executive_revenue_forecast.py \
	$(ROOT)/build_executive_pipeline_risk_process.py \
	$(ROOT)/build_executive_customer_risk_growth.py \
	$(ROOT)/build_forecast_revenue_motions.py \
	$(ROOT)/build_executive_product_mix_industry.py \
	$(ROOT)/build_product_portfolio_dashboard.py \
	$(ROOT)/build_product_ml_recommendations.py \
	$(ROOT)/build_bdr_operating_dashboards.py \
	$(ROOT)/build_customer_account_health.py \
	$(ROOT)/build_lead_funnel.py \
	$(ROOT)/build_contract_operations_renewals.py \
	$(ROOT)/build_revenue_pipeline_analyst_lab.py \
	$(ROOT)/build_customer_revenue_analyst_lab.py \
	$(ROOT)/build_pipeline_opportunity_operations.py \
	$(ROOT)/build_pipeline_history.py
SCRIPT_FILES := $(wildcard $(ROOT)/scripts/*.py)

.PHONY: all verify verify-static lint compile contracts alerts-dry-run \
	readiness api-smoke security-audit api-version-check \
	model-drift-dry-run validate-actions telemetry-dry-run \
	metrics-drift-dry-run export-live-revenue upgrade-exec-revenue-live \
	intelligence-validate intelligence-inventory \
	builder-brain-validate builder-brain-inventory \
	builder-brain-handoffs-validate builder-brain-handoffs-inventory \
	builder-brain-live-smoke builder-brain-live-smoke-report \
	extract-kpis autopilot-run focus-loop continue-8h continue-8h-split

FOCUS_KEYS ?= forecast_revenue_motions
FOCUS_HOURS ?= 8
FOCUS_SESSION ?= default
SPLIT_FOCUS_KEYS_A ?= forecast_revenue_motions
SPLIT_FOCUS_KEYS_B ?= revenue_retention_health
SPLIT_FOCUS_HOURS_A ?= 4
SPLIT_FOCUS_HOURS_B ?= 4
BUILDER_BRAIN_LIVE_SMOKE_DIR ?= $(ROOT)/config/builder_brain_live_smoke
BUILDER_BRAIN_LIVE_SMOKE_TARGET_ORG ?= apro@simcorp.com
BUILDER_BRAIN_LIVE_SMOKE_TIMEOUT ?= 180
BUILDER_BRAIN_LIVE_SMOKE_OUTPUT_DIR ?= $(ROOT)/output/builder_brain/live_smoke
BUILDER_BRAIN_LIVE_SMOKE_REPORT_MANIFEST ?= $(BUILDER_BRAIN_LIVE_SMOKE_DIR)/probe_matrix_report_live_smoke.json
BUILDER_BRAIN_LIVE_SMOKE_MIXED_MANIFEST ?= $(BUILDER_BRAIN_LIVE_SMOKE_DIR)/probe_matrix_mixed_live_smoke.json

all:
	cd $(ROOT) && $(PYTHON) build_executive_revenue_forecast.py
	cd $(ROOT) && $(PYTHON) build_pipeline_opportunity_operations.py
	cd $(ROOT) && $(PYTHON) build_forecast_revenue_motions.py
	cd $(ROOT) && $(PYTHON) build_executive_product_mix_industry.py
	cd $(ROOT) && $(PYTHON) build_product_portfolio_dashboard.py
	cd $(ROOT) && $(PYTHON) build_product_ml_recommendations.py
	cd $(ROOT) && $(PYTHON) build_bdr_operating_dashboards.py
	cd $(ROOT) && $(PYTHON) build_customer_account_health.py
	cd $(ROOT) && $(PYTHON) build_lead_funnel.py
	cd $(ROOT) && $(PYTHON) build_contract_operations_renewals.py
	cd $(ROOT) && $(PYTHON) build_executive_pipeline_risk_process.py
	cd $(ROOT) && $(PYTHON) build_executive_customer_risk_growth.py
	cd $(ROOT) && $(PYTHON) build_revenue_pipeline_analyst_lab.py
	cd $(ROOT) && $(PYTHON) build_customer_revenue_analyst_lab.py

# ─── Phase 0: Static verification ────────────────────────────────────
verify: verify-static

verify-static: lint compile contracts

lint:
	$(PYTHON) -m ruff check $(CORE_FILES) $(SCRIPT_FILES)

compile:
	@for f in $(CORE_FILES) $(SCRIPT_FILES); do \
		$(PYTHON) -m py_compile "$$f" || exit 1; \
	done
	@echo "All files compile OK"

contracts:
	cd $(ROOT) && $(PYTHON) scripts/contract_lint.py

intelligence-validate:
	cd $(ROOT) && $(PYTHON) scripts/analytics_intelligence.py validate

intelligence-inventory:
	cd $(ROOT) && $(PYTHON) scripts/analytics_intelligence.py inventory

builder-brain-validate:
	cd $(ROOT) && $(PYTHON) scripts/builder_brain.py validate

builder-brain-inventory:
	cd $(ROOT) && $(PYTHON) scripts/builder_brain.py inventory

builder-brain-handoffs-validate:
	cd $(ROOT) && $(PYTHON) scripts/builder_brain_handoff_targets.py validate

builder-brain-handoffs-inventory:
	cd $(ROOT) && $(PYTHON) scripts/builder_brain_handoff_targets.py inventory

builder-brain-live-smoke-report:
	cd $(ROOT) && $(PYTHON) scripts/builder_brain.py probe-matrix \
		--manifest $(BUILDER_BRAIN_LIVE_SMOKE_REPORT_MANIFEST) \
		--target-org $(BUILDER_BRAIN_LIVE_SMOKE_TARGET_ORG) \
		--executor-timeout-seconds $(BUILDER_BRAIN_LIVE_SMOKE_TIMEOUT) \
		--output-dir $(BUILDER_BRAIN_LIVE_SMOKE_OUTPUT_DIR)/report \
		--json

builder-brain-live-smoke:
	cd $(ROOT) && $(PYTHON) scripts/builder_brain.py probe-matrix \
		--manifest $(BUILDER_BRAIN_LIVE_SMOKE_MIXED_MANIFEST) \
		--target-org $(BUILDER_BRAIN_LIVE_SMOKE_TARGET_ORG) \
		--executor-timeout-seconds $(BUILDER_BRAIN_LIVE_SMOKE_TIMEOUT) \
		--output-dir $(BUILDER_BRAIN_LIVE_SMOKE_OUTPUT_DIR)/mixed \
		--json

# ─── Phase 0: Org readiness ──────────────────────────────────────────
readiness:
	cd $(ROOT) && $(PYTHON) scripts/org_readiness_scan.py

api-smoke:
	cd $(ROOT) && $(PYTHON) scripts/api_smoke_matrix.py

# ─── Phase 1: Security and API governance ────────────────────────────
security-audit:
	cd $(ROOT) && $(PYTHON) scripts/security_coverage_audit.py

api-version-check:
	cd $(ROOT) && $(PYTHON) scripts/api_version_monitor.py

# ─── Phase 3: Alerts ─────────────────────────────────────────────────
alerts-dry-run:
	cd $(ROOT) && $(PYTHON) scripts/alerts_router.py --dry-run

# ─── Phase 2: Action layer validation ─────────────────────────────
validate-actions:
	cd $(ROOT) && $(PYTHON) scripts/interaction_validator.py

# ─── Phase 3: Telemetry ────────────────────────────────────────────
telemetry-dry-run:
	cd $(ROOT) && $(PYTHON) scripts/adoption_telemetry_report.py --dry-run

# ─── Phase 4: ML/AI Model Governance ────────────────────────────────
model-drift-dry-run:
	cd $(ROOT) && $(PYTHON) scripts/model_drift_report.py --dry-run

# ─── Phase 5: Semantic metric drift ────────────────────────────────
metrics-drift-dry-run:
	cd $(ROOT) && $(PYTHON) scripts/metrics_drift_check.py --dry-run

export-live-revenue:
	cd $(ROOT) && $(PYTHON) scripts/export_live_crma_assets.py \
		"Executive Revenue & Forecast" \
		"Forecast & Revenue Motions"

upgrade-exec-revenue-live:
	cd $(ROOT) && $(PYTHON) scripts/upgrade_executive_revenue_live.py
	cd $(ROOT) && $(PYTHON) scripts/export_live_crma_assets.py "Executive Revenue & Forecast"

extract-kpis:
	cd $(ROOT) && $(PYTHON) scripts/extract_kpi_workbook.py /Users/test/Downloads/Metrics_and_KPIs_updated.xlsx

# Monthly Sales Directors Review end-to-end pipeline.
#   make monthly-review                  runs today's snapshot
#   make monthly-review DATE=2026-04-15  backdates a run
#   make monthly-review-analysis-only    skips extract + decks
.PHONY: monthly-review monthly-review-analysis-only monthly-review-decks-only
DATE ?= $(shell date +%Y-%m-%d)
monthly-review:
	cd $(ROOT) && $(PYTHON) scripts/run_monthly_director_review.py --date $(DATE)

monthly-review-analysis-only:
	cd $(ROOT) && $(PYTHON) scripts/run_monthly_director_review.py --date $(DATE) \
		--skip-extract --skip-decks

monthly-review-decks-only:
	cd $(ROOT) && $(PYTHON) scripts/run_monthly_director_review.py --date $(DATE) \
		--skip-extract --skip-analysis

autopilot-run:
	cd $(ROOT) && $(PYTHON) scripts/run_dashboard_autopilot.py --session default

focus-loop:
	cd $(ROOT) && $(PYTHON) scripts/run_focus_loop.py \
		--keys $(FOCUS_KEYS) \
		--hours $(FOCUS_HOURS) \
		--session $(FOCUS_SESSION)

continue-8h:
	cd $(ROOT) && $(PYTHON) scripts/run_focus_loop.py \
		--keys forecast_revenue_motions \
		--hours 8 \
		--session default

continue-8h-split:
	cd $(ROOT) && $(PYTHON) scripts/run_focus_loop.py \
		--keys $(SPLIT_FOCUS_KEYS_A) \
		--hours $(SPLIT_FOCUS_HOURS_A) \
		--session $(FOCUS_SESSION) \
		--label split_a_$(SPLIT_FOCUS_KEYS_A)
	cd $(ROOT) && $(PYTHON) scripts/run_focus_loop.py \
		--keys $(SPLIT_FOCUS_KEYS_B) \
		--hours $(SPLIT_FOCUS_HOURS_B) \
		--session $(FOCUS_SESSION) \
		--label split_b_$(SPLIT_FOCUS_KEYS_B)
