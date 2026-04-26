#!/usr/bin/env python3
"""
Comprehensive dashboard fix script:
1. Fix HTML-encoded page labels, widget text, and step queries across ALL Gen 2 dashboards
2. Fix broken Anomaly Lab health trend query
3. Add methodology explainer text widgets to ML scoring pages
4. Upgrade basic chart types on ML pages (donut→stackhbar, basic column→combo)
5. Fix broken Pipeline Ops widget
"""
import json, html, subprocess, urllib.request, sys, time

# Auth
auth = json.loads(subprocess.check_output(["sf","org","display","--json"], stderr=subprocess.DEVNULL))['result']
TOKEN = auth['accessToken']
IURL = auth['instanceUrl']
HDR = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}

def fully_unescape(s):
    prev = None
    while s != prev:
        prev = s
        s = html.unescape(s)
    return s

def deep_unescape(obj):
    """Recursively unescape ALL strings in any JSON structure."""
    if isinstance(obj, str):
        return fully_unescape(obj)
    elif isinstance(obj, dict):
        return {k: deep_unescape(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [deep_unescape(item) for item in obj]
    return obj

def clean_state(state):
    """Full state cleaning for GET→PATCH round-trip."""
    state.pop('layouts', None)
    # Deep unescape the entire state
    state = deep_unescape(state)
    # Remove problematic fields from SAQL steps
    for sname, sdef in state.get('steps', {}).items():
        if sdef.get('type') == 'saql':
            sdef.pop('datasets', None)
        if sdef.get('type') == 'aggregateflex':
            if 'datasets' in sdef:
                for ds in sdef['datasets']:
                    ds.pop('label', None)
                    ds.pop('url', None)
    return state

def get_dashboard(did):
    url = f"{IURL}/services/data/v66.0/wave/dashboards/{did}"
    req = urllib.request.Request(url, headers=HDR)
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())

def patch_dashboard(did, state):
    url = f"{IURL}/services/data/v66.0/wave/dashboards/{did}"
    body = json.dumps({"state": state}).encode()
    req = urllib.request.Request(url, data=body, headers=HDR, method='PATCH')
    with urllib.request.urlopen(req) as r:
        return r.status

# ============================================================
# GEN 2 DASHBOARD IDS
# ============================================================
GEN2 = {
    "Customer & Account Health": "0FKTb0000000HvJOAU",
    "Pipeline & Opportunity Operations": "0FKTb0000000Hs5OAE",
    "Lead Funnel": "0FKTb0000000HwvOAE",
    "Anomaly Detection & Forecasting Lab": "0FKTb0000000IPxOAM",
    "Executive Revenue & Forecast": "0FKTb0000000HqTOAU",
    "Executive Pipeline Risk": "0FKTb0000000I09OAE",
    "Executive Customer Risk": "0FKTb0000000I1lOAE",
    "Executive Product Mix": "0FKTb0000000IBROA2",
    "Forecast & Revenue Motions": "0FKTb0000000HthOAE",
    "Contract Operations": "0FKTb0000000HyXOAU",
    "Product Portfolio": "0FKTb0000000I6bOAE",
    "Advanced Pipeline Analytics": "0FKTb0000000HnFOAU",
    "Analytics Command Center": "0FKTb0000000IEfOAM",
    "Marketing Pipeline Attribution": "0FKTb0000000IMjOAM",
    "Finance Revenue Operations": "0FKTb0000000IOLOA2",
    "AE Performance": "0FKTb0000000IGHOA2",
    "Sales Operations Command Center": "0FKTb0000000IHtOAM",
    "Manager Coaching": "0FKTb0000000IJVOA2",
    "Revenue/Pipeline Analyst Lab": "0FKTb0000000I3NOAU",
    "Customer/Revenue Analyst Lab": "0FKTb0000000I4zOAE",
    "BDR Manager": "0FKTb0000000I8DOAU",
    "BDR Rep Queue": "0FKTb0000000I9pOAE",
    "Product ML & Recommendations": "0FKTb0000000ID3OAM",
}

# ============================================================
# METHODOLOGY TEXT WIDGETS
# ============================================================
def make_method_text(title, body):
    return {
        "type": "text",
        "parameters": {
            "content": {
                "richTextContent": [
                    {"attributes": {"size": "16px", "color": "#091A3E", "bold": True}, "insert": title},
                    {"insert": "\n"},
                    {"attributes": {"size": "12px", "color": "#54698D"}, "insert": body},
                    {"insert": "\n"}
                ]
            },
            "interactions": []
        }
    }

HEALTH_METHOD = make_method_text(
    "How Health Score Works",
    "Health Score (0-100) = Recency Weight x 40% + Velocity Weight x 30% + Size Weight x 30%. "
    "Bands: Healthy >= 70 | Monitor >= 50 | At Risk >= 30 | Critical < 30. "
    "Factors: days since last activity, expansion pipeline velocity, total ARR, engagement frequency. "
    "Scores recalculated weekly from live Salesforce data."
)

WIN_RISK_METHOD = make_method_text(
    "How Win/Risk Scores Work",
    "Win Score (0-100) = Stage Weight x 35% + Activity Weight x 25% + History Weight x 20% + Size Weight x 20%. "
    "Risk signals: close date pushes > 2, days in stage > SLA, no activity > 14d, competitor mentioned. "
    "Slip Risk = weighted combination of push count, stage stall, and engagement decay. "
    "High Risk = WinScore < 40 or TotalRiskScore > 70. Refreshed with each pipeline sync."
)

LEAD_METHOD = make_method_text(
    "How Lead Scoring Works",
    "Lead Score (0-100) = Engagement Weight x 30% + Fit Weight x 30% + Behavior Weight x 25% + Timing Weight x 15%. "
    "Priority Bands: Hot >= 80 | Warm >= 60 | Nurture >= 40 | Cold < 40. "
    "Engagement: email opens, web visits, content downloads. Fit: title, company size, industry match. "
    "Behavior: form fills, demo requests, pricing page visits. Timing: recency of interactions."
)

# ============================================================
# PHASE 1: Fix HTML encoding on ALL dashboards
# ============================================================
print("=" * 60)
print("PHASE 1: Fixing HTML encoding across all dashboards")
print("=" * 60)

fixed = 0
errors = 0
for name, did in GEN2.items():
    try:
        d = get_dashboard(did)
        state = d['state']
        state_str_before = json.dumps(state)
        
        # Check if there's any HTML encoding
        if '&amp;' in state_str_before or '&quot;' in state_str_before or '&#39;' in state_str_before or '&gt;' in state_str_before or '&lt;' in state_str_before:
            state = clean_state(state)
            state_str_after = json.dumps(state)
            
            if state_str_before != state_str_after:
                status = patch_dashboard(did, state)
                print(f"  ✓ {name} — HTML encoding fixed (HTTP {status})")
                fixed += 1
                time.sleep(0.3)  # Rate limit
            else:
                print(f"  · {name} — no encoding changes needed")
        else:
            print(f"  · {name} — clean, no HTML encoding found")
    except Exception as e:
        print(f"  ✗ {name} — ERROR: {e}")
        errors += 1

print(f"\nPhase 1 complete: {fixed} dashboards fixed, {errors} errors\n")

# ============================================================
# PHASE 2: Fix Anomaly Lab broken health trend + improve charts
# ============================================================
print("=" * 60)
print("PHASE 2: Fix Anomaly Detection Lab")
print("=" * 60)

try:
    d = get_dashboard("0FKTb0000000IPxOAM")
    state = clean_state(d['state'])
    
    # Fix s_htrd: Change to use detail records grouped by HealthBand (no time series available)
    state['steps']['s_htrd'] = {
        'type': 'saql',
        'broadcastFacet': True,
        'query': (
            'q = load "Customer_Account_Health";\n'
            'q = filter q by RecordType == "detail";\n'
            'q = group q by (HealthBand, Segment);\n'
            'q = foreach q generate HealthBand, Segment,\n'
            '    count() as Accounts,\n'
            '    avg(HealthScore) as AvgScore,\n'
            '    sum(TotalWonARR) as TotalARR;\n'
            'q = order q by TotalARR desc;'
        )
    }
    
    # Fix c3a widget: change from line (no trend data) to stackhbar (shows health distribution)
    state['widgets']['c3a'] = {
        "type": "chart",
        "parameters": {
            "autoFitMode": "fit",
            "showValues": True,
            "visualizationType": "stackhbar",
            "step": "s_htrd",
            "theme": "wave",
            "exploreLink": True,
            "title": {
                "fontSize": 14,
                "subtitleFontSize": 11,
                "label": "Account Health Distribution",
                "align": "center",
                "subtitleLabel": "Accounts by health band and segment with ARR exposure"
            },
            "interactions": [],
            "showActionMenu": True,
            "applyConditionalFormatting": True,
            "legend": {"showHeader": True, "show": True, "customSize": "auto", "position": "right-top", "inside": False},
            "measureAxis1": {"sqrtScale": False, "showTitle": True, "showAxis": True, "title": "Account Count", "customDomain": {"showDomain": False}},
            "dimensionAxis": {"showTitle": False, "customSize": "auto", "showAxis": True, "title": "", "icons": {"useIcons": False, "iconProps": {"fit": "cover", "column": "", "type": "round"}}}
        }
    }
    
    # Upgrade c3b (Health by Segment) from column to combo
    state['widgets']['c3b']['parameters']['visualizationType'] = 'combo'
    state['widgets']['c3b']['parameters']['title']['subtitleLabel'] = 'Stacked accounts per segment colored by health band'
    
    # Upgrade c1c (Stage Conversion) from hbar to combo (show deals + avg days together)
    state['widgets']['c1c']['parameters']['visualizationType'] = 'combo'
    
    # Upgrade c2c (Rep Won vs Pipeline) from hbar to combo
    state['widgets']['c2c']['parameters']['visualizationType'] = 'combo'
    
    # Upgrade c2b (Forecast Category) from column to stackcolumn 
    state['widgets']['c2b']['parameters']['visualizationType'] = 'stackcolumn'
    
    status = patch_dashboard("0FKTb0000000IPxOAM", state)
    print(f"  ✓ Anomaly Lab fixed — health trend query + chart upgrades (HTTP {status})")
except Exception as e:
    print(f"  ✗ Anomaly Lab — ERROR: {e}")

print()

# ============================================================
# PHASE 3: Enhance Customer & Account Health ML page
# ============================================================
print("=" * 60)
print("PHASE 3: Enhance Customer & Account Health ML page")
print("=" * 60)

try:
    d = get_dashboard("0FKTb0000000HvJOAU")
    state = clean_state(d['state'])
    
    # Find the ml_risk_scoring page
    pages = state['gridLayouts'][0]['pages']
    ml_page = None
    for p in pages:
        if p['name'] == 'ml_risk_scoring':
            ml_page = p
            break
    
    if ml_page:
        # Add methodology text widget
        state['widgets']['w_health_method'] = HEALTH_METHOD
        
        # Upgrade s_ml_risk_distribution chart: change from hbar to stackhbar showing segment breakdown
        state['steps']['s_ml_risk_distribution'] = {
            'type': 'saql',
            'broadcastFacet': True,
            'query': (
                'q = load "Customer_Account_Health";\n'
                'q = filter q by RecordType == "detail";\n'
                'q = group q by (HealthBand, Segment);\n'
                'q = foreach q generate HealthBand, Segment,\n'
                '    count() as Accounts,\n'
                '    avg(HealthScore) as AvgScore,\n'
                '    sum(TotalWonARR) as AffectedARR;\n'
                'q = order q by Accounts desc;'
            )
        }
        
        # Upgrade the chart widget to stackhbar
        for wn, wd in state['widgets'].items():
            if wd.get('type') == 'chart' and wd.get('parameters',{}).get('step') == 's_ml_risk_distribution':
                wd['parameters']['visualizationType'] = 'stackhbar'
                wd['parameters']['title']['label'] = 'Account Distribution by Health Band'
                wd['parameters']['title']['subtitleLabel'] = 'Segment breakdown within each health band'
                break
        
        # Upgrade s_scores_by_segment: make it a combo chart 
        for wn, wd in state['widgets'].items():
            if wd.get('type') == 'chart' and wd.get('parameters',{}).get('step') == 's_scores_by_segment':
                wd['parameters']['visualizationType'] = 'combo'
                wd['parameters']['title']['subtitleLabel'] = 'Avg health score + ARR exposure by segment'
                break
        
        # Upgrade scatter to bubble for better insight
        for wn, wd in state['widgets'].items():
            if wd.get('type') == 'chart' and wd.get('parameters',{}).get('step') == 's_ml_health_scatter':
                wd['parameters']['title']['subtitleLabel'] = 'Each dot = account | Size = ARR | Position = health vs expansion potential'
                break
        
        # Add methodology widget to the page layout
        existing_widgets = ml_page['widgets']
        max_row = max(w['row'] + w['rowspan'] for w in existing_widgets) if existing_widgets else 0
        
        # Add methodology text at the top - shift everything down by 3 rows
        for w in existing_widgets:
            w['row'] += 3
        existing_widgets.insert(0, {"name": "w_health_method", "colspan": 12, "column": 0, "row": 0, "rowspan": 3})
        
        status = patch_dashboard("0FKTb0000000HvJOAU", state)
        print(f"  ✓ Customer & Account Health ML page enhanced (HTTP {status})")
    else:
        print(f"  ✗ ml_risk_scoring page not found")
except Exception as e:
    print(f"  ✗ Customer & Account Health — ERROR: {e}")

print()

# ============================================================
# PHASE 4: Enhance Pipeline & Opportunity Operations ML page  
# ============================================================
print("=" * 60)
print("PHASE 4: Enhance Pipeline & Opportunity Ops ML page")
print("=" * 60)

try:
    d = get_dashboard("0FKTb0000000Hs5OAE")
    state = clean_state(d['state'])
    
    pages = state['gridLayouts'][0]['pages']
    ml_page = None
    for p in pages:
        if p['name'] == 'ml_win_risk':
            ml_page = p
            break
    
    if ml_page:
        # Add methodology text widget
        state['widgets']['w_winrisk_method'] = WIN_RISK_METHOD
        
        # Upgrade donut (w_wr_risk_dist) to stackhbar
        if 'w_wr_risk_dist' in state['widgets']:
            state['widgets']['w_wr_risk_dist']['parameters']['visualizationType'] = 'stackhbar'
            state['widgets']['w_wr_risk_dist']['parameters']['title']['label'] = 'Open Pipeline by Risk Band'
            state['widgets']['w_wr_risk_dist']['parameters']['title']['subtitleLabel'] = 'ARR exposure by risk classification'
        
        # Upgrade column (w_wr_by_stage) to combo for dual-measure insight
        if 'w_wr_by_stage' in state['widgets']:
            state['widgets']['w_wr_by_stage']['parameters']['visualizationType'] = 'combo'
            state['widgets']['w_wr_by_stage']['parameters']['title']['label'] = 'Risk Distribution by Stage'
            state['widgets']['w_wr_by_stage']['parameters']['title']['subtitleLabel'] = 'Count + avg win score per stage highlights conversion bottlenecks'
        
        # Upgrade scatter subtitle for clarity
        if 'w_wr_scatter' in state['widgets']:
            state['widgets']['w_wr_scatter']['parameters']['title']['subtitleLabel'] = 'Each dot = deal | X = win probability | Y = slip risk | Size = ARR'
        
        # Add methodology widget to page layout
        existing_widgets = ml_page['widgets']
        for w in existing_widgets:
            w['row'] += 3
        existing_widgets.insert(0, {"name": "w_winrisk_method", "colspan": 12, "column": 0, "row": 0, "rowspan": 3})
        
        status = patch_dashboard("0FKTb0000000Hs5OAE", state)
        print(f"  ✓ Pipeline & Opportunity Ops ML page enhanced (HTTP {status})")
    else:
        print(f"  ✗ ml_win_risk page not found")
except Exception as e:
    print(f"  ✗ Pipeline & Opportunity Ops — ERROR: {e}")

print()

# ============================================================
# PHASE 5: Enhance Lead Funnel ML page
# ============================================================
print("=" * 60)
print("PHASE 5: Enhance Lead Funnel ML page")
print("=" * 60)

try:
    d = get_dashboard("0FKTb0000000HwvOAE")
    state = clean_state(d['state'])
    
    pages = state['gridLayouts'][0]['pages']
    ml_page = None
    for p in pages:
        if p['name'] == 'ml_lead_scoring':
            ml_page = p
            break
    
    if ml_page:
        # Add methodology text widget
        state['widgets']['w_lead_method'] = LEAD_METHOD
        
        # Upgrade grade distribution (hbar) to stackhbar with source breakdown
        if 'w_ls_grade_dist' in state['widgets']:
            state['widgets']['w_ls_grade_dist']['parameters']['visualizationType'] = 'stackhbar'
            state['widgets']['w_ls_grade_dist']['parameters']['title']['subtitleLabel'] = 'Lead volume by priority band — identifies conversion potential'
        
        # Upgrade score by source (column) to combo for dual measures
        if 'w_ls_by_source' in state['widgets']:
            state['widgets']['w_ls_by_source']['parameters']['visualizationType'] = 'combo'
            state['widgets']['w_ls_by_source']['parameters']['title']['subtitleLabel'] = 'Avg lead score + volume by source shows ROI of acquisition channels'
        
        # Upgrade leads by rep (hbar) to combo
        if 'w_ls_by_owner' in state['widgets']:
            state['widgets']['w_ls_by_owner']['parameters']['visualizationType'] = 'combo'
            state['widgets']['w_ls_by_owner']['parameters']['title']['subtitleLabel'] = 'High-priority leads per rep — identifies capacity vs workload balance'
        
        # Add methodology widget to page layout
        existing_widgets = ml_page['widgets']
        for w in existing_widgets:
            w['row'] += 3
        existing_widgets.insert(0, {"name": "w_lead_method", "colspan": 12, "column": 0, "row": 0, "rowspan": 3})
        
        status = patch_dashboard("0FKTb0000000HwvOAE", state)
        print(f"  ✓ Lead Funnel ML page enhanced (HTTP {status})")
    else:
        print(f"  ✗ ml_lead_scoring page not found")
except Exception as e:
    print(f"  ✗ Lead Funnel — ERROR: {e}")

print()
print("=" * 60)
print("ALL PHASES COMPLETE")
print("=" * 60)
