# CRM Analytics

Salesforce CRM Analytics dashboard suite — programmatic dashboard builders that deploy directly to CRM Analytics via the Wave API.

## Dashboards

| Dashboard                       | Builder                          | Widgets | Pages | Description                                                                                                                                             |
| ------------------------------- | -------------------------------- | ------- | ----- | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Advanced Pipeline Analytics** | `build_advanced_analytics.py`    | 172     | 7     | ML-powered pipeline intelligence with GradientBoosting win probability, Markov chain analysis, survival curves, deal archetypes, FY revenue forecasting |
| **Book of Business**            | `build_dashboard.py`             | 102     | 5     | Core pipeline dashboard with push analysis, win probability, stage bottlenecks                                                                          |
| **Customer Intelligence**       | `build_customer_intelligence.py` | ~120    | 6     | Account health scoring, churn risk, expansion signals                                                                                                   |
| **Revenue Motions**             | `build_revenue_motions.py`       | ~100    | 5     | Revenue attribution, motion analysis, conversion funnels                                                                                                |
| **Account Intelligence**        | `build_account_intelligence.py`  | ~80     | 4     | Account-level analytics, whitespace analysis                                                                                                            |
| **Sales Compliance**            | `build_sales_compliance.py`      | ~70     | 4     | Forecast accuracy, data hygiene, process compliance                                                                                                     |
| **Lead Management**             | `build_lead_management.py`       | ~60     | 3     | Lead scoring, routing analysis, conversion tracking                                                                                                     |
| **Contract Operations**         | `build_contract_operations.py`   | ~50     | 3     | Contract lifecycle, renewal tracking                                                                                                                    |
| **Forecasting**                 | `build_forecasting.py`           | ~40     | 2     | Forecast rollup, accuracy trending                                                                                                                      |
| **Pipeline History**            | `build_pipeline_history.py`      | ~25     | 1     | Historical pipeline snapshots                                                                                                                           |

## Architecture

All dashboards share `crm_analytics_helpers.py` (~1,900 lines), which provides:

- **Widget builders**: `rich_chart()`, `combo_chart()`, `timeline_chart()`, `heatmap_chart()`, `sankey_chart()`, `bullet_chart()`, `gauge_chart()`, `waterfall_chart()`, `funnel_chart()`, `treemap_chart()`, `scatter_chart()`, `bubble_chart()`, `area_chart()`, `line_chart()`
- **Layout helpers**: `nav_row()`, `nav_link()`, `section_label()`, `hdr()`, `num()`, `pillbox()`
- **SAQL helpers**: `sq()`, `coalesce_filter()`, `aggregate_filter()`
- **Dataset management**: `upload_dataset()`, `create_dashboard_if_needed()`, `deploy_dashboard()`
- **Salesforce API**: `sf_query()`, `sf_auth()`, `set_record_links_xmd()`

## Advanced Pipeline Analytics (flagship)

The `build_advanced_analytics.py` dashboard (~4,300 lines) includes:

### ML Models

- **GradientBoosting Win Probability** — 5-fold CV, AUC-ROC ~0.95, permutation importance
- **Slip Risk Scoring** — logistic regression on push history features
- **Timing Score** — probability of closing this quarter
- **Markov Chain Stage Transitions** — absorbing Markov chain with fundamental matrix inversion
- **Kaplan-Meier Survival Analysis** — time-to-close survival curves by stage group
- **K-Means Deal Archetypes** — unsupervised clustering into 4 deal types
- **Monte Carlo Revenue Simulation** — 10K iterations with VaR/CVaR

### 7 Dashboard Pages

1. **Executive Summary** — KPIs, pipeline trend, Monte Carlo forecast, FY revenue, action queue
2. **Deal Push Intelligence** — push frequency, commit-stage pushes, impact analysis
3. **Win Probability** — model performance (AUC-ROC gauge), feature importance, calibration
4. **Stage Bottleneck** — funnel conversion, backward moves, stage velocity
5. **Deals Won** — cohort analysis, lead source treemap, deal size distribution
6. **Trendlines & Revenue Forecast** — FY closed vs projected (quarterly stacked + combo), pipeline/win rate trends, YTD running totals, moving averages
7. **Quantitative Intelligence** — Markov absorption, transition heatmap, survival curves, deal archetypes, momentum distribution, VaR waterfall

### Chart Types Used

area, bubble, bullet, column, combo, comparisontable, funnel, gauge, hbar, heatmap, line, sankey, scatter, stackcolumn, timeline, treemap, waterfall

## Prerequisites

- Python 3.10+
- Salesforce CLI (`sf`) authenticated to your org
- `scikit-learn` for ML models

```bash
pip install -r requirements.txt
```

## Usage

```bash
# Deploy Advanced Pipeline Analytics
python build_advanced_analytics.py

# Deploy Book of Business
python build_dashboard.py

# Deploy all dashboards
make all
```

Each builder:

1. Authenticates via `sf` CLI
2. Queries Salesforce (Opportunities, OpportunityHistory, etc.)
3. Runs ML models and computes analytics
4. Uploads datasets to CRM Analytics
5. Builds and deploys the dashboard JSON via PATCH API
6. Sets XMD record links for drill-through navigation

## Configuration

Authentication uses the Salesforce CLI:

```bash
sf org login web --alias myorg
sf config set target-org myorg
```

No `.env` files or hardcoded credentials — the builders call `sf org display` at runtime.
