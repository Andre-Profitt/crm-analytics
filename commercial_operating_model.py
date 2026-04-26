#!/usr/bin/env python3
"""Shared commercial operating model for SimCorp CRM Analytics builders.

This module centralizes the handbook-backed commercial model so dashboards do
not each reinvent role, motion, cadence, and stage logic.
"""

from __future__ import annotations

import re
from typing import Any


STAGE_GATE_MODEL = [
    {
        "stage": 1,
        "label": "Prospecting",
        "persona": "Sales",
        "required_signals": [
            "engaged prospect",
            "current system understood",
            "need identified",
            "investment context understood",
        ],
    },
    {
        "stage": 2,
        "label": "Discovery",
        "persona": "Sales",
        "required_signals": [
            "active meeting cadence",
            "PAIC complete",
            "decision makers identified",
            "timeline visible",
            "scope identified",
        ],
    },
    {
        "stage": 3,
        "label": "Engagement",
        "persona": "Sales",
        "required_signals": [
            "buying process clearer",
            "business case forming",
            "timeline and budget clearer",
            "TAS complete",
            "relationship map built",
        ],
    },
    {
        "stage": 4,
        "label": "Shortlisted",
        "persona": "Sales",
        "required_signals": [
            "competitive position established",
            "shortlist confirmed",
            "close plan emerging",
        ],
    },
    {
        "stage": 5,
        "label": "Preferred",
        "persona": "Sales",
        "required_signals": [
            "preferred vendor trajectory",
            "close plan created",
            "commercial/legal workshops aligned",
        ],
    },
    {
        "stage": 6,
        "label": "Contracting",
        "persona": "Sales",
        "required_signals": [
            "redlines received",
            "final commercial terms aligned",
            "implementation terms aligned",
        ],
    },
    {
        "stage": 7,
        "label": "Opt-out",
        "persona": "CX",
        "required_signals": [
            "termination / no-opportunity path explicit",
            "save-plan eligibility reviewed",
            "service/contract transition understood",
        ],
    },
    {
        "stage": 8,
        "label": "Won",
        "persona": "Shared",
        "required_signals": [
            "signed",
            "booked in Salesforce",
            "handover complete",
            "delivery / CSM transition complete",
        ],
    },
]


MOTION_OWNERSHIP = {
    "Land": {
        "primary_persona": "Sales",
        "secondary_personas": ["Marketing", "BDR"],
        "dashboard_family": "Sales Manager",
    },
    "Expand": {
        "primary_persona": "CX",
        "secondary_personas": ["Sales", "Services"],
        "dashboard_family": "CSM Manager",
    },
    "Renewal": {
        "primary_persona": "CX",
        "secondary_personas": ["Services"],
        "dashboard_family": "CSM Manager",
    },
    "Contraction": {
        "primary_persona": "CX",
        "secondary_personas": ["Services"],
        "dashboard_family": "CSM Manager",
    },
    "Churn": {
        "primary_persona": "CX",
        "secondary_personas": ["Services"],
        "dashboard_family": "CSM Manager",
    },
}


OPERATING_CADENCE = {
    "Executive": {
        "cadence": "weekly to monthly",
        "questions": [
            "Are we on target?",
            "Where is confidence weak?",
            "Which accounts or regions need intervention?",
        ],
    },
    "Sales Manager": {
        "cadence": "weekly",
        "questions": [
            "Which deals need promotion?",
            "Which commits need protection?",
            "Which omitted deals need cleanup?",
            "Which reps need coaching?",
        ],
    },
    "CSM Manager": {
        "cadence": "weekly to monthly",
        "questions": [
            "Which renewals are at risk?",
            "Where is protected base weak?",
            "Which accounts need save plans or QBR intervention?",
            "Where is growth on base developing?",
        ],
    },
    "Individual": {
        "cadence": "daily",
        "questions": [
            "What do I work on next?",
            "Which records need follow-up?",
            "Which risks need escalation today?",
        ],
    },
}


SHARED_DRILL_TARGETS = {
    "Account360": {
        "label": "Account 360 & History",
        "role": "shared diagnostic hub",
        "used_by": ["Executive", "Sales Manager", "CSM Manager", "Individual", "Product / GTM"],
    },
    "OpportunityDetail": {
        "label": "Opportunity Detail",
        "role": "forecast and qualification inspection",
        "used_by": ["Sales Manager", "Executive", "Individual"],
    },
    "RenewalDetail": {
        "label": "Renewal Detail",
        "role": "renewal risk and save-plan inspection",
        "used_by": ["CSM Manager", "Executive", "Individual"],
    },
}


DASHBOARD_CONTRACTS = {
    "Forecast & Revenue Motions": {
        "persona": "Sales Manager",
        "must_answer": [
            "forecast confidence",
            "promotion pressure",
            "commit protection",
            "omitted cleanup",
            "rep coaching pressure",
            "deal review / test-and-improve candidates",
        ],
    },
    "Revenue Retention & Health": {
        "persona": "CSM Manager",
        "must_answer": [
            "strict renewal performance",
            "effective retention / protected base",
            "growth on base",
            "churn risk",
            "save-plan pressure",
            "QBR / governance cadence",
        ],
    },
    "Account 360 & History": {
        "persona": "Shared",
        "must_answer": [
            "full account economic story",
            "legacy context vs modern ARR context",
            "product movement",
            "renewal / expansion / churn evidence",
        ],
    },
}


def _normalize_text(*parts: str | None) -> str:
    return " | ".join(part.strip() for part in parts if part and part.strip()).lower()


def classify_persona(
    *,
    title: str | None = None,
    user_role: str | None = None,
    department: str | None = None,
    division: str | None = None,
) -> str:
    """Classify a user into a commercial persona.

    This intentionally prefers CRM Analytics dashboard semantics over strict HR
    semantics because titles and roles are messy in the live org.
    """

    text = _normalize_text(title, user_role, department, division)
    if "marketing" in text:
        return "Marketing"
    if "sales" in text and "customer success" not in text and not re.search(r"\bcx\b", text):
        return "Sales"
    if "customer success" in text or re.search(r"\bcx\b", text) or "customer experience" in text:
        return "CX"
    if (
        "consult" in text
        or "professional services" in text
        or "service delivery" in text
        or "delivery center" in text
        or "testing services" in text
        or "support" in text
        or "opportunity owners" in text
    ):
        return "Services"
    if "vice president" in text or "managing director" in text:
        return "Leadership"
    return "Other"


def classify_region(*, user_role: str | None = None, department: str | None = None, division: str | None = None) -> str:
    text = _normalize_text(user_role, department, division)
    if "north america" in text or "sc na" in text:
        return "North America"
    if "emea" in text or "sc ne" in text or "sc se" in text or "sc ce" in text or "uk & me" in text:
        return "EMEA"
    if "asia" in text or "apac" in text or "sc asia" in text:
        return "APAC"
    return "Unknown"


def primary_motion_persona(motion: str | None) -> str:
    return MOTION_OWNERSHIP.get((motion or "").strip(), {}).get("primary_persona", "Unknown")


def ownership_alignment(owner_persona: str | None, motion: str | None) -> str:
    motion_key = (motion or "").strip()
    owner_key = (owner_persona or "").strip() or "Unknown"
    if owner_key in {"Unknown", "Other"} or not motion_key:
        return "Unclassified"
    config = MOTION_OWNERSHIP.get(motion_key)
    if not config:
        return "Unclassified"
    if owner_key == config["primary_persona"]:
        return "Aligned"
    if owner_key in config.get("secondary_personas", []):
        return "Shared"
    return "Needs Review"


def dashboard_persona(dashboard_label: str) -> str:
    return DASHBOARD_CONTRACTS.get(dashboard_label, {}).get("persona", "Unknown")


def role_dimension_row(
    *,
    owner_id: str,
    owner_name: str,
    title: str | None = None,
    user_role: str | None = None,
    department: str | None = None,
    division: str | None = None,
    manager_id: str | None = None,
    manager_name: str | None = None,
) -> dict[str, Any]:
    persona = classify_persona(
        title=title,
        user_role=user_role,
        department=department,
        division=division,
    )
    return {
        "OwnerId": owner_id or "",
        "OwnerName": owner_name or "",
        "ManagerId": manager_id or "",
        "ManagerName": manager_name or "",
        "Title": title or "",
        "UserRole": user_role or "",
        "Department": department or "",
        "Division": division or "",
        "Persona": persona,
        "Region": classify_region(user_role=user_role, department=department, division=division),
    }
