PYTHON ?= python3

ROOT := $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
CORE_FILES := \
	$(ROOT)/crm_analytics_helpers.py \
	$(ROOT)/build_dashboard.py \
	$(ROOT)/build_revenue_motions.py \
	$(ROOT)/build_sales_compliance.py \
	$(ROOT)/build_customer_intelligence.py \
	$(ROOT)/build_account_intelligence.py \
	$(ROOT)/build_lead_management.py \
	$(ROOT)/build_contract_operations.py \
	$(ROOT)/build_forecasting.py \
	$(ROOT)/build_pipeline_history.py
SCRIPT_FILES := \
	$(ROOT)/scripts/_loader.py \
	$(ROOT)/scripts/contract_lint.py \
	$(ROOT)/scripts/smoke_runner.py \
	$(ROOT)/scripts/deploy_orchestrator.py \
	$(ROOT)/scripts/metrics_drift_check.py \
	$(ROOT)/scripts/alerts_router.py \
	$(ROOT)/scripts/org_readiness_scan.py \
	$(ROOT)/scripts/api_smoke_matrix.py \
	$(ROOT)/scripts/security_coverage_audit.py \
	$(ROOT)/scripts/api_version_monitor.py \
	$(ROOT)/scripts/model_drift_report.py \
	$(ROOT)/scripts/interaction_validator.py \
	$(ROOT)/scripts/adoption_telemetry_report.py

.PHONY: verify verify-static lint compile contracts alerts-dry-run \
	readiness api-smoke security-audit api-version-check \
	model-drift-dry-run validate-actions telemetry-dry-run \
	metrics-drift-dry-run builder-brain-live-smoke builder-brain-live-smoke-report

BUILDER_BRAIN_LIVE_SMOKE_DIR ?= $(ROOT)/config/builder_brain_live_smoke
BUILDER_BRAIN_LIVE_SMOKE_TARGET_ORG ?= apro@simcorp.com
BUILDER_BRAIN_LIVE_SMOKE_TIMEOUT ?= 180
BUILDER_BRAIN_LIVE_SMOKE_OUTPUT_DIR ?= $(ROOT)/output/builder_brain/live_smoke
BUILDER_BRAIN_LIVE_SMOKE_REPORT_MANIFEST ?= $(BUILDER_BRAIN_LIVE_SMOKE_DIR)/probe_matrix_report_live_smoke.json
BUILDER_BRAIN_LIVE_SMOKE_MIXED_MANIFEST ?= $(BUILDER_BRAIN_LIVE_SMOKE_DIR)/probe_matrix_mixed_live_smoke.json

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
