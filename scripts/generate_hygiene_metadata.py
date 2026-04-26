"""
Generate Metadata API XML for the 5 Hygiene custom objects.

Writes force-app/main/default/objects/{Object}__c/{Object}__c.object-meta.xml
plus per-field XML under /fields/.

Run: python3 scripts/generate_hygiene_metadata.py
Then: sf project deploy validate --source-dir force-app/main/default/objects/Hygiene_Snapshot__c
      sf project deploy start    --source-dir force-app/main/default/objects/Hygiene_Snapshot__c
"""

from __future__ import annotations

from pathlib import Path
from textwrap import dedent

ROOT = Path(__file__).resolve().parents[1]
OBJECTS_DIR = ROOT / "force-app" / "main" / "default" / "objects"


# Field helpers ──────────────────────────────────────────────────────────────


def _f(name, ftype, label, **attrs):
    """Return (filename, xml) for a custom field."""
    # Common attributes
    external_id = "false"
    required = "false"
    unique = "false"
    tracking = "false"

    # type-specific body
    extras = []
    if ftype == "Text":
        extras.append(f"<length>{attrs.get('length', 255)}</length>")
    elif ftype == "LongTextArea":
        extras.append(f"<length>{attrs.get('length', 32000)}</length>")
        extras.append(f"<visibleLines>{attrs.get('visibleLines', 3)}</visibleLines>")
    elif ftype == "Number":
        extras.append(f"<precision>{attrs.get('precision', 18)}</precision>")
        extras.append(f"<scale>{attrs.get('scale', 0)}</scale>")
    elif ftype == "Currency":
        extras.append(f"<precision>{attrs.get('precision', 18)}</precision>")
        extras.append(f"<scale>{attrs.get('scale', 2)}</scale>")
    elif ftype == "Date":
        pass
    elif ftype == "DateTime":
        pass
    elif ftype == "Checkbox":
        extras.append(f"<defaultValue>{attrs.get('default', 'false')}</defaultValue>")
    elif ftype == "Picklist":
        values = attrs.get("values", [])
        value_xml = "\n        ".join(
            f"<value><fullName>{v}</fullName><default>false</default><label>{v}</label></value>"
            for v in values
        )
        extras.append(
            f"""<valueSet>
      <valueSetDefinition>
        <sorted>false</sorted>
        {value_xml}
      </valueSetDefinition>
    </valueSet>"""
        )
    elif ftype == "Lookup":
        extras.append(
            f"<referenceTo>{attrs['referenceTo']}</referenceTo>\n    "
            f"<relationshipName>{attrs.get('relationshipName', name.replace('__c', '').replace('_', ''))}</relationshipName>\n    "
            "<deleteConstraint>SetNull</deleteConstraint>"
        )

    if attrs.get("required"):
        required = "true"
    if attrs.get("unique"):
        unique = "true"
    if attrs.get("external_id"):
        external_id = "true"

    body = "\n    ".join(extras) if extras else ""

    xml = dedent(
        f"""\
        <?xml version="1.0" encoding="UTF-8"?>
        <CustomField xmlns="http://soap.sforce.com/2006/04/metadata">
          <fullName>{name}</fullName>
          <label>{label}</label>
          <type>{ftype}</type>
          <externalId>{external_id}</externalId>
          <required>{required}</required>
          <trackTrending>{tracking}</trackTrending>
          {body}
        </CustomField>
        """
    )
    return (name + ".field-meta.xml", xml)


def _object_xml(label, plural, description, has_name_field=True):
    name_field = ""
    if has_name_field:
        name_field = dedent(
            """\
            <nameField>
              <label>Auto Number</label>
              <type>AutoNumber</type>
              <displayFormat>HYG-{0000000}</displayFormat>
              <trackHistory>false</trackHistory>
            </nameField>"""
        )
    return dedent(
        f"""\
        <?xml version="1.0" encoding="UTF-8"?>
        <CustomObject xmlns="http://soap.sforce.com/2006/04/metadata">
          <label>{label}</label>
          <pluralLabel>{plural}</pluralLabel>
          <description>{description}</description>
          <sharingModel>ReadWrite</sharingModel>
          <deploymentStatus>Deployed</deploymentStatus>
          <enableActivities>false</enableActivities>
          <enableHistory>false</enableHistory>
          <enableReports>true</enableReports>
          <enableSearch>false</enableSearch>
          <enableFeeds>false</enableFeeds>
          <enableBulkApi>true</enableBulkApi>
          <enableSharing>true</enableSharing>
          <enableStreamingApi>true</enableStreamingApi>
          {name_field}
        </CustomObject>
        """
    )


# Common severity + grain picklist values used in multiple tables
SEVERITIES = ["Critical", "Important", "Domain", "Baseline"]
GRAINS = ["deal", "account", "installation", "quote", "closed_deal", "whitespace"]


OBJECTS = {
    "Hygiene_Snapshot__c": {
        "label": "Hygiene Snapshot",
        "plural": "Hygiene Snapshots",
        "description": (
            "Aggregate data-quality / hygiene count per (run_date, metric_key). "
            "Each row drives a KPI tile or trend line on the Sales Ops dashboard."
        ),
        "fields": [
            _f("Run_Date__c", "Date", "Run Date", required=True),
            _f("Metric_Key__c", "Text", "Metric Key", length=80, required=True),
            _f("Metric_Label__c", "Text", "Metric Label", length=255),
            _f("Severity__c", "Picklist", "Severity", values=SEVERITIES),
            _f("Category__c", "Text", "Category", length=80),
            _f("Grain__c", "Picklist", "Grain", values=GRAINS),
            _f("Count__c", "Number", "Count", precision=10),
            _f("Prior_Count__c", "Number", "Prior Run Count", precision=10),
            _f("Delta__c", "Number", "Delta (curr - prior)", precision=10),
            _f(
                "SF_Logic__c",
                "LongTextArea",
                "SF Logic (rule)",
                length=2000,
                visibleLines=3,
            ),
            _f(
                "Action__c",
                "LongTextArea",
                "Recommended Action",
                length=2000,
                visibleLines=3,
            ),
            _f("Is_Hero_Alert__c", "Checkbox", "Is Hero Alert?"),
        ],
    },
    "Hygiene_Deal_Flag__c": {
        "label": "Hygiene Deal Flag",
        "plural": "Hygiene Deal Flags",
        "description": (
            "Per-opportunity hygiene finding. One row per (opportunity, metric_key, run_date). "
            "Dimensions denormalized on the row so reports GROUP BY region/owner/etc. natively."
        ),
        "fields": [
            _f("Run_Date__c", "Date", "Run Date", required=True),
            _f("Metric_Key__c", "Text", "Metric Key", length=80, required=True),
            _f("Severity__c", "Picklist", "Severity", values=SEVERITIES),
            _f("Category__c", "Text", "Category", length=80),
            _f(
                "Opportunity__c",
                "Lookup",
                "Opportunity",
                referenceTo="Opportunity",
                relationshipName="HygieneDealFlags",
            ),
            _f(
                "Account__c",
                "Lookup",
                "Account",
                referenceTo="Account",
                relationshipName="HygieneDealFlags",
            ),
            _f(
                "Owner_User__c",
                "Lookup",
                "Owner (User)",
                referenceTo="User",
                relationshipName="HygieneDealFlagsOwned",
            ),
            _f("Director__c", "Text", "Director Name (denorm)", length=120),
            _f("Region__c", "Text", "Region (denorm)", length=80),
            _f("Territory__c", "Text", "Territory (denorm)", length=80),
            _f("Industry__c", "Text", "Industry (denorm)", length=80),
            _f("ARR__c", "Currency", "ARR (Unweighted)"),
            _f("Push_Count__c", "Number", "Push Count", precision=5),
            _f("Stage__c", "Text", "Stage", length=80),
            _f("First_Seen_Date__c", "Date", "First Seen in Metric"),
            _f("Streak_Runs__c", "Number", "Streak Runs (consecutive)", precision=5),
        ],
    },
    "Hygiene_Account_Flag__c": {
        "label": "Hygiene Account Flag",
        "plural": "Hygiene Account Flags",
        "description": (
            "Per-account hygiene finding. Used for KYC, NDA, Short Code, "
            "contract-expired, AUM-stale, and whitespace signals."
        ),
        "fields": [
            _f("Run_Date__c", "Date", "Run Date", required=True),
            _f("Metric_Key__c", "Text", "Metric Key", length=80, required=True),
            _f("Severity__c", "Picklist", "Severity", values=SEVERITIES),
            _f("Category__c", "Text", "Category", length=80),
            _f(
                "Account__c",
                "Lookup",
                "Account",
                referenceTo="Account",
                relationshipName="HygieneAccountFlags",
            ),
            _f(
                "Owner_User__c",
                "Lookup",
                "Owner (User)",
                referenceTo="User",
                relationshipName="HygieneAccountFlagsOwned",
            ),
            _f("Region__c", "Text", "Region (denorm)", length=80),
            _f("Territory__c", "Text", "Territory (denorm)", length=80),
            _f("Industry__c", "Text", "Industry (denorm)", length=80),
            _f("AuM__c", "Number", "AuM (denorm, b)", precision=10, scale=2),
            _f("First_Seen_Date__c", "Date", "First Seen in Metric"),
            _f("Streak_Runs__c", "Number", "Streak Runs", precision=5),
        ],
    },
    "Hygiene_Installation_Flag__c": {
        "label": "Hygiene Installation Flag",
        "plural": "Hygiene Installation Flags",
        "description": (
            "Per-installation hygiene finding. Ghost installations, overlapping "
            "contracts, asset-vs-renewal ARR mismatches."
        ),
        "fields": [
            _f("Run_Date__c", "Date", "Run Date", required=True),
            _f("Metric_Key__c", "Text", "Metric Key", length=80, required=True),
            _f("Severity__c", "Picklist", "Severity", values=SEVERITIES),
            _f(
                "Installation__c",
                "Lookup",
                "Installation",
                referenceTo="Installation__c",
                relationshipName="HygieneInstallFlags",
            ),
            _f(
                "Account__c",
                "Lookup",
                "Account",
                referenceTo="Account",
                relationshipName="HygieneInstallFlags",
            ),
            _f("Status__c", "Text", "Installation Status (denorm)", length=80),
            _f("Extended_To_Date__c", "Date", "ExtendedToDate (denorm)"),
        ],
    },
    "Hygiene_Quote_Flag__c": {
        "label": "Hygiene Quote Flag",
        "plural": "Hygiene Quote Flags",
        "description": (
            "Per-Apttus-proposal hygiene finding. Stale quotes, stuck approvals."
        ),
        "fields": [
            _f("Run_Date__c", "Date", "Run Date", required=True),
            _f("Metric_Key__c", "Text", "Metric Key", length=80, required=True),
            _f("Severity__c", "Picklist", "Severity", values=SEVERITIES),
            _f(
                "Quote__c",
                "Lookup",
                "Apttus Proposal",
                referenceTo="Apttus_Proposal__Proposal__c",
                relationshipName="HygieneQuoteFlags",
            ),
            _f(
                "Opportunity__c",
                "Lookup",
                "Opportunity",
                referenceTo="Opportunity",
                relationshipName="HygieneQuoteFlags",
            ),
            _f(
                "Account__c",
                "Lookup",
                "Account",
                referenceTo="Account",
                relationshipName="HygieneQuoteFlags",
            ),
            _f("Approval_Stage__c", "Text", "Approval Stage (denorm)", length=80),
            _f("Age_Days__c", "Number", "Age (days since created)", precision=6),
            _f(
                "Last_Modified_Days__c",
                "Number",
                "Days Since Last Modified",
                precision=6,
            ),
        ],
    },
}


def main():
    for obj_name, spec in OBJECTS.items():
        obj_dir = OBJECTS_DIR / obj_name
        (obj_dir / "fields").mkdir(parents=True, exist_ok=True)
        # Write object definition
        obj_xml = _object_xml(
            spec["label"], spec["plural"], spec["description"], has_name_field=True
        )
        (obj_dir / f"{obj_name}.object-meta.xml").write_text(obj_xml)
        # Write each field
        for fname, fxml in spec["fields"]:
            (obj_dir / "fields" / fname).write_text(fxml)
        n_fields = len(spec["fields"])
        print(f"  wrote {obj_name}  ({n_fields} fields)")
    print()
    print(
        f"Total: {len(OBJECTS)} objects generated at {OBJECTS_DIR.relative_to(ROOT)}/"
    )
    print()
    print("Next:")
    print(
        "  sf project deploy validate --source-dir force-app/main/default/objects "
        "--target-org apro@simcorp.com"
    )


if __name__ == "__main__":
    main()
