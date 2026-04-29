#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import importlib
import io
import json
import os
import shutil
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient


APP_ROOT = Path(__file__).resolve().parents[1]
PROCESSED_ROOT = APP_ROOT / "processed"
GOLDEN_PATH = APP_ROOT / "tools" / "integrity_golden_cases.json"
DEFAULT_QUERY_PARAMS = {
    "timeline_event_types": ["current_detected", "recently_added"],
    "timeline_date_field": "first_seen",
    "migration_timing_operator": "and",
    "page": 1,
    "page_size": 100,
    "sort_by": "total_score",
    "sort_direction": "desc",
}
ADVANCED_SCOPE_FIELDS = {
    "marketingPlatforms": "marketing_platforms",
    "crmPlatforms": "crm_platforms",
    "paymentPlatforms": "payment_platforms",
    "hostingProviders": "hosting_providers",
    "agencies": "agencies",
    "aiTools": "ai_tools",
    "complianceFlags": "compliance_flags",
}


@dataclass
class CaseResult:
    case_id: str
    description: str
    mismatches: list[str]
    leads_total: int
    export_rows: int
    analytics_filtered_leads: int
    sample_domains: list[str]
    timeline_unique_domains: int | None
    scoped_option_checks: dict[str, int]
    selected_only_total: int | None = None

    @property
    def ok(self) -> bool:
        return not self.mismatches


def build_query_pairs(params: dict[str, Any]) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    merged = {**DEFAULT_QUERY_PARAMS, **params}
    for key, value in merged.items():
        if value in (None, "", False):
            continue
        if isinstance(value, list):
            for item in value:
                if item not in (None, ""):
                    pairs.append((key, str(item)))
            continue
        if value is True:
            pairs.append((key, "true"))
            continue
        pairs.append((key, str(value)))
    return pairs


def split_pipe(value: str | None) -> list[str]:
    if not value:
        return []
    seen: set[str] = set()
    items: list[str] = []
    for item in value.split("|"):
        normalized = item.strip()
        if normalized and normalized not in seen:
            seen.add(normalized)
            items.append(normalized)
    return items


def parse_csv_rows(content: str) -> list[dict[str, str]]:
    reader = csv.DictReader(io.StringIO(content))
    return list(reader)


def create_temp_data_root() -> Path:
    temp_root = Path(tempfile.mkdtemp(prefix="domain-dealer-audit-"))
    for filename in ("builtwith.db", "summary.json", "filter_options.json"):
        source = PROCESSED_ROOT / filename
        target = temp_root / filename
        try:
            target.symlink_to(source)
        except OSError:
            shutil.copy2(source, target)
    state_db = PROCESSED_ROOT / "lead_console_state.db"
    if state_db.exists():
        shutil.copy2(state_db, temp_root / "lead_console_state.db")
    return temp_root


def load_client(temp_root: Path) -> TestClient:
    sys.path.insert(0, str(APP_ROOT))
    os.environ["DOMAIN_DEALER_DATA_ROOT"] = str(temp_root)
    if "backend.main" in sys.modules:
        del sys.modules["backend.main"]
    module = importlib.import_module("backend.main")
    module = importlib.reload(module)
    return TestClient(module.app)


def expected_subset(rows: list[dict[str, str]], field_name: str) -> set[str]:
    values: set[str] = set()
    for row in rows:
        values.update(split_pipe(row.get(field_name)))
    return values


def run_case(client: TestClient, case: dict[str, Any]) -> CaseResult:
    params = build_query_pairs(case["params"])
    mismatches: list[str] = []

    leads_response = client.get("/api/leads", params=params)
    leads_response.raise_for_status()
    leads_payload = leads_response.json()
    lead_items = leads_payload["items"]
    leads_total = int(leads_payload["total"])

    export_response = client.get("/api/leads/export", params=params)
    export_response.raise_for_status()
    export_rows = parse_csv_rows(export_response.text)

    analytics_response = client.get("/api/analytics", params=params)
    analytics_response.raise_for_status()
    analytics_payload = analytics_response.json()

    filter_options_response = client.get("/api/filter-options", params=params)
    filter_options_response.raise_for_status()
    filter_options = filter_options_response.json()

    if leads_total != len(export_rows):
        mismatches.append(f"worksheet/export count mismatch: leads={leads_total}, export={len(export_rows)}")
    if analytics_payload["kpis"]["filteredLeads"] != leads_total:
        mismatches.append(
            "worksheet/analytics count mismatch: "
            f"leads={leads_total}, analytics={analytics_payload['kpis']['filteredLeads']}"
        )

    lead_sample = [item["root_domain"] for item in lead_items[:5]]
    export_sample = [row["root_domain"] for row in export_rows[:5]]
    if export_sample[: len(lead_sample)] != lead_sample:
        mismatches.append(f"page/export sample mismatch: leads={lead_sample}, export={export_sample[:len(lead_sample)]}")

    if lead_items:
        root_domain = lead_items[0]["root_domain"]
        detail_response = client.get(f"/api/leads/{root_domain}")
        detail_response.raise_for_status()
        detail = detail_response.json()
        detail_lead = detail["lead"]
        for field in ("cms_migration_status", "cms_migration_likely_date", "best_old_domain", "domain_migration_status"):
            if str(detail_lead.get(field, "")) != str(lead_items[0].get(field, "")):
                mismatches.append(f"drawer/worksheet mismatch for {root_domain}: field={field}")
        if lead_items[0].get("se_ranking_analysis_type"):
            for field in (
                "se_ranking_analysis_type",
                "se_ranking_analysis_mode",
                "se_ranking_market",
                "se_ranking_first_month",
                "se_ranking_second_month",
                "se_ranking_checked_at",
                "se_ranking_status",
            ):
                if str(detail_lead.get(field, "")) != str(lead_items[0].get(field, "")):
                    mismatches.append(f"drawer/worksheet SE mismatch for {root_domain}: field={field}")
            if lead_items[0].get("se_ranking_analysis_mode") == "manual":
                for field in ("se_ranking_date_label_first", "se_ranking_date_label_second"):
                    if str(detail_lead.get(field, "")) != str(lead_items[0].get(field, "")):
                        mismatches.append(f"drawer/worksheet manual SE mismatch for {root_domain}: field={field}")

    scoped_option_checks: dict[str, int] = {}
    for option_key, export_field in ADVANCED_SCOPE_FIELDS.items():
        scoped_values = set(filter_options.get(option_key, []))
        export_values = expected_subset(export_rows, export_field)
        extras = sorted(scoped_values - export_values)
        scoped_option_checks[option_key] = len(scoped_values)
        if extras:
            mismatches.append(f"scoped options mismatch for {option_key}: {extras[:5]}")

    timeline_unique_domains: int | None = None
    if case.get("expect_timeline_parity") and any(key == "timeline_platforms" for key, _value in params):
        timeline_response = client.get("/api/timeline/cohort", params=params)
        timeline_response.raise_for_status()
        timeline_payload = timeline_response.json()
        timeline_unique_domains = int(timeline_payload["summary"]["uniqueDomains"])
        if timeline_unique_domains != leads_total:
            mismatches.append(
                "worksheet/timeline mismatch: "
                f"leads={leads_total}, timeline_unique_domains={timeline_unique_domains}"
            )

    selected_only_total: int | None = None
    if case.get("exercise_select_filtered"):
        clear_response = client.post("/api/export-tray/clear", json={})
        clear_response.raise_for_status()
        select_response = client.post("/api/export-tray/select-filtered", params=params, json={})
        select_response.raise_for_status()
        select_payload = select_response.json()
        if int(select_payload.get("matchedCount", -1)) != leads_total:
            mismatches.append(
                "select-filtered matchedCount mismatch: "
                f"matched={select_payload.get('matchedCount')}, leads={leads_total}"
            )
        selected_pairs = build_query_pairs({**case["params"], "selected_only": True})
        selected_leads_response = client.get("/api/leads", params=selected_pairs)
        selected_leads_response.raise_for_status()
        selected_only_total = int(selected_leads_response.json()["total"])
        if selected_only_total != leads_total:
            mismatches.append(
                "selected-only mismatch after tray mutation: "
                f"selected_only={selected_only_total}, leads={leads_total}"
            )
        selected_export_response = client.get("/api/leads/export", params=selected_pairs)
        selected_export_response.raise_for_status()
        selected_export_rows = parse_csv_rows(selected_export_response.text)
        if len(selected_export_rows) != leads_total:
            mismatches.append(
                "selected-only export mismatch: "
                f"selected_export={len(selected_export_rows)}, leads={leads_total}"
            )

    return CaseResult(
        case_id=case["id"],
        description=case["description"],
        mismatches=mismatches,
        leads_total=leads_total,
        export_rows=len(export_rows),
        analytics_filtered_leads=int(analytics_payload["kpis"]["filteredLeads"]),
        sample_domains=export_sample[:5],
        timeline_unique_domains=timeline_unique_domains,
        scoped_option_checks=scoped_option_checks,
        selected_only_total=selected_only_total,
    )


def render_markdown(results: list[CaseResult]) -> str:
    lines = [
        "# Integrity Audit Report",
        "",
        "| Case | Status | Leads | Export | Analytics | Notes |",
        "| --- | --- | ---: | ---: | ---: | --- |",
    ]
    for result in results:
        note = "OK" if result.ok else "<br>".join(result.mismatches[:3])
        lines.append(
            f"| {result.case_id} | {'PASS' if result.ok else 'FAIL'} | {result.leads_total} | "
            f"{result.export_rows} | {result.analytics_filtered_leads} | {note} |"
        )
    return "\n".join(lines) + "\n"


def load_cases() -> dict[str, Any]:
    return json.loads(GOLDEN_PATH.read_text(encoding="utf-8"))


def update_expectations(payload: dict[str, Any], results: list[CaseResult]) -> dict[str, Any]:
    result_map = {result.case_id: result for result in results}
    for case in payload["cases"]:
        result = result_map[case["id"]]
        case["expectations"] = {
            "leadsTotal": result.leads_total,
            "exportRows": result.export_rows,
            "analyticsFilteredLeads": result.analytics_filtered_leads,
            "sampleDomains": result.sample_domains,
            "timelineUniqueDomains": result.timeline_unique_domains,
            "selectedOnlyTotal": result.selected_only_total,
        }
    return payload


def compare_expectations(payload: dict[str, Any], results: list[CaseResult]) -> list[str]:
    failures: list[str] = []
    result_map = {result.case_id: result for result in results}
    for case in payload["cases"]:
        expected = case.get("expectations", {})
        result = result_map[case["id"]]
        if expected.get("leadsTotal") != result.leads_total:
            failures.append(f"{case['id']}: expected leads {expected.get('leadsTotal')}, got {result.leads_total}")
        if expected.get("exportRows") != result.export_rows:
            failures.append(f"{case['id']}: expected export rows {expected.get('exportRows')}, got {result.export_rows}")
        if expected.get("analyticsFilteredLeads") != result.analytics_filtered_leads:
            failures.append(
                f"{case['id']}: expected analytics {expected.get('analyticsFilteredLeads')}, got {result.analytics_filtered_leads}"
            )
        if expected.get("sampleDomains") != result.sample_domains:
            failures.append(f"{case['id']}: sample domains changed from {expected.get('sampleDomains')} to {result.sample_domains}")
        if expected.get("timelineUniqueDomains") != result.timeline_unique_domains:
            failures.append(
                f"{case['id']}: expected timeline unique domains {expected.get('timelineUniqueDomains')}, "
                f"got {result.timeline_unique_domains}"
            )
        if expected.get("selectedOnlyTotal") != result.selected_only_total:
            failures.append(
                f"{case['id']}: expected selected-only total {expected.get('selectedOnlyTotal')}, got {result.selected_only_total}"
            )
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the DOMAIN DEALER integrity audit harness.")
    parser.add_argument("--update-golden", action="store_true", help="Rewrite the golden case expectations from current results.")
    parser.add_argument("--markdown-out", type=Path, help="Optional markdown output path.")
    parser.add_argument("--json-out", type=Path, help="Optional JSON output path.")
    args = parser.parse_args()

    payload = load_cases()
    temp_root = create_temp_data_root()
    try:
        client = load_client(temp_root)
        results = [run_case(client, case) for case in payload["cases"]]
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)

    markdown = render_markdown(results)
    if args.markdown_out:
        args.markdown_out.write_text(markdown, encoding="utf-8")
    else:
        print(markdown)

    if args.json_out:
        args.json_out.write_text(
            json.dumps(
                {
                    "results": [
                        {
                            "caseId": result.case_id,
                            "description": result.description,
                            "ok": result.ok,
                            "mismatches": result.mismatches,
                            "leadsTotal": result.leads_total,
                            "exportRows": result.export_rows,
                            "analyticsFilteredLeads": result.analytics_filtered_leads,
                            "sampleDomains": result.sample_domains,
                            "timelineUniqueDomains": result.timeline_unique_domains,
                            "selectedOnlyTotal": result.selected_only_total,
                            "scopedOptionChecks": result.scoped_option_checks,
                        }
                        for result in results
                    ]
                },
                indent=2,
            ),
            encoding="utf-8",
        )

    if args.update_golden:
        updated_payload = update_expectations(payload, results)
        GOLDEN_PATH.write_text(json.dumps(updated_payload, indent=2) + "\n", encoding="utf-8")
        return 0

    failures = [mismatch for result in results for mismatch in result.mismatches]
    failures.extend(compare_expectations(payload, results))
    if failures:
        print("\nIntegrity audit failures:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
