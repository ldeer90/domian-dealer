#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import sqlite3
import subprocess
import sys
import tempfile
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


APP_ROOT = Path(__file__).resolve().parents[1]
TOOLS_ROOT = APP_ROOT / "tools"
STATE_DB = APP_ROOT / "processed" / "lead_console_state.db"
INTEGRITY_SCRIPT = TOOLS_ROOT / "audit_integrity.py"
FRONTEND_APP = APP_ROOT / "frontend" / "src" / "App.tsx"
OUTPUT_MARKDOWN = APP_ROOT / "FULL_STACK_PIPELINE_AUDIT.md"
OUTPUT_JSON = TOOLS_ROOT / "pipeline_audit_report.json"


def now_iso() -> str:
    return datetime.now().astimezone().isoformat()


def split_pipe(value: str | None) -> list[str]:
    return [item.strip() for item in (value or "").split("|") if item.strip()]


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def parse_ts_array(source: str, const_name: str) -> list[str]:
    pattern = re.compile(rf"{re.escape(const_name)}[^=]*=\s*\[(.*?)\];", re.DOTALL)
    match = pattern.search(source)
    if not match:
        return []
    return re.findall(r'"([^"]+)"', match.group(1))


def run_integrity_harness() -> dict[str, Any]:
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as handle:
        temp_path = Path(handle.name)
    try:
        completed = subprocess.run(
            [sys.executable, str(INTEGRITY_SCRIPT), "--json-out", str(temp_path)],
            cwd=str(APP_ROOT),
            capture_output=True,
            text=True,
        )
        payload = json.loads(temp_path.read_text(encoding="utf-8")) if temp_path.exists() and temp_path.stat().st_size else {"results": []}
        payload["exitCode"] = completed.returncode
        payload["stderr"] = completed.stderr.strip()
        payload["stdout"] = completed.stdout.strip()
        return payload
    finally:
        temp_path.unlink(missing_ok=True)


def table_columns(connection: sqlite3.Connection, table: str) -> list[dict[str, str]]:
    return [
        {"name": row[1], "type": row[2]}
        for row in connection.execute(f"pragma table_info({table})").fetchall()
    ]


def query_counter(connection: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> dict[str, int]:
    rows = connection.execute(sql, params).fetchall()
    return {str(key): int(value) for key, value in rows}


def top_pipe_values(connection: sqlite3.Connection, table: str, column: str, *, limit: int = 10, where: str = "") -> dict[str, int]:
    counter: Counter[str] = Counter()
    sql = f"select {column} from {table}"
    if where:
        sql += f" where {where}"
    for (value,) in connection.execute(sql).fetchall():
        for item in split_pipe(value):
            counter[item] += 1
    return dict(counter.most_common(limit))


def fetch_single_value(connection: sqlite3.Connection, sql: str) -> int:
    row = connection.execute(sql).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def build_pipeline_contract() -> list[dict[str, Any]]:
    return [
        {
            "pipeline": "SE Ranking",
            "discovery": "Tray-driven or manual comparison input -> latest snapshot per root_domain selected in join",
            "extraction": [
                "analysis_type",
                "analysis_mode",
                "regional_source",
                "traffic deltas",
                "keyword deltas",
                "outcome_flags",
                "captured_at/status/error",
            ],
            "scoring": "Used mainly as supporting evidence and worksheet sorting, not as a deep additive score layer",
            "presentation": "Worksheet columns, drawer summary, export, filters",
            "segmentation": "Supports momentum/change buckets but can drift if latest-result semantics are unclear",
        },
        {
            "pipeline": "Site status",
            "discovery": "Direct HTTP request to selected tray domains with follow-redirect classification",
            "extraction": [
                "requested_url",
                "final_url",
                "status_code",
                "status_category",
                "redirect_count",
                "checked_at",
                "error_message",
            ],
            "scoring": "Currently more cleanup-oriented than score-driving",
            "presentation": "Worksheet, filters, drawer, export",
            "segmentation": "Useful for dead/redirect cleanup and live-site exclusions; weak as a standalone outreach signal",
        },
        {
            "pipeline": "Screaming Frog",
            "discovery": "CMS-aware seed discovery -> redirect-aware homepage -> sitemap/category ranking -> bounded seed cap",
            "extraction": [
                "crawl/result quality",
                "title/meta/canonical/H1 flags",
                "schema/internal error counts",
                "collection intelligence",
                "heading intelligence",
                "seed diagnostics",
            ],
            "scoring": "Primary enrichment score layer via opportunity score, issue family, issue reason, outreach hooks, lead score bonus",
            "presentation": "Worksheet, drawer, full audit workspace, export, filters",
            "segmentation": "Strongest audit pipeline for cold-email segmentation, but also the highest-risk for extraction and scoring drift",
        },
    ]


def build_signal_inventory(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    return [
        {
            "pipeline": "SE Ranking",
            "signalFamilies": [
                "analysis mode/type",
                "traffic deltas",
                "keyword deltas",
                "price deltas",
                "outcome flags",
                "status/error",
            ],
            "snapshotRows": fetch_single_value(connection, "select count(*) from seranking_analysis_snapshots"),
            "latestStatusCounts": query_counter(connection, "select coalesce(status, ''), count(*) from seranking_analysis_snapshots group by 1 order by 2 desc"),
            "topOutcomeFlags": top_pipe_values(connection, "seranking_analysis_snapshots", "outcome_flags"),
        },
        {
            "pipeline": "Site status",
            "signalFamilies": [
                "status category",
                "status code",
                "redirect count",
                "final URL",
                "error",
            ],
            "snapshotRows": fetch_single_value(connection, "select count(*) from site_status_snapshots"),
            "statusCategoryCounts": query_counter(connection, "select coalesce(status_category, ''), count(*) from site_status_snapshots group by 1 order by 2 desc"),
        },
        {
            "pipeline": "Screaming Frog",
            "signalFamilies": [
                "crawl quality",
                "seed diagnostics",
                "technical issue flags",
                "collection intelligence",
                "heading intelligence",
                "opportunity scoring",
            ],
            "snapshotRows": fetch_single_value(connection, "select count(*) from screamingfrog_audit_snapshots"),
            "statusCounts": query_counter(connection, "select coalesce(status, ''), count(*) from screamingfrog_audit_snapshots group by 1 order by 2 desc"),
            "resultQualityCounts": query_counter(connection, "select coalesce(result_quality, ''), count(*) from screamingfrog_audit_snapshots group by 1 order by 2 desc"),
            "resultReasonCounts": query_counter(connection, "select coalesce(result_reason, ''), count(*) from screamingfrog_audit_snapshots group by 1 order by 2 desc limit 10"),
            "primaryIssueFamilies": query_counter(connection, "select coalesce(sf_primary_issue_family, ''), count(*) from screamingfrog_audit_snapshots group by 1 order by 2 desc"),
            "collectionIntroStatuses": query_counter(connection, "select coalesce(collection_intro_status, ''), count(*) from screamingfrog_audit_snapshots group by 1 order by 2 desc"),
            "titleOptimizationStatuses": query_counter(connection, "select coalesce(title_optimization_status, ''), count(*) from screamingfrog_audit_snapshots group by 1 order by 2 desc"),
            "topSchemaIssueFlags": top_pipe_values(connection, "screamingfrog_audit_snapshots", "schema_issue_flags"),
            "topCollectionIssueFlags": top_pipe_values(connection, "screamingfrog_audit_snapshots", "collection_content_issue_flags"),
            "topDefaultTitleFlags": top_pipe_values(connection, "screamingfrog_audit_snapshots", "default_title_issue_flags"),
            "topHeadingIssueFlags": top_pipe_values(connection, "screamingfrog_audit_snapshots", "heading_issue_flags"),
        },
    ]


def build_scoring_matrix() -> list[dict[str, str]]:
    return [
        {"field": "homepage_status_category", "pipeline": "Screaming Frog", "weight": "High", "issueFamily": "technical_breakage", "outreachRelevance": "High"},
        {"field": "internal_4xx_count/internal_5xx_count", "pipeline": "Screaming Frog", "weight": "High", "issueFamily": "technical_breakage", "outreachRelevance": "High"},
        {"field": "schema_page_count", "pipeline": "Screaming Frog", "weight": "Medium", "issueFamily": "schema_gap", "outreachRelevance": "Medium"},
        {"field": "collection_intro_status", "pipeline": "Screaming Frog", "weight": "High", "issueFamily": "collection_content_gap", "outreachRelevance": "High"},
        {"field": "title_optimization_status", "pipeline": "Screaming Frog", "weight": "Medium", "issueFamily": "default_collection_title", "outreachRelevance": "High"},
        {"field": "title_issue_flags/meta_issue_flags", "pipeline": "Screaming Frog", "weight": "Medium", "issueFamily": "product_metadata_gap", "outreachRelevance": "Medium"},
        {"field": "heading_issue_flags", "pipeline": "Screaming Frog", "weight": "Medium", "issueFamily": "heading_hygiene", "outreachRelevance": "Medium"},
        {"field": "traffic_delta_percent/keywords_delta_percent", "pipeline": "SE Ranking", "weight": "Support", "issueFamily": "Outcome only", "outreachRelevance": "Medium"},
        {"field": "site_status_category", "pipeline": "Site status", "weight": "Support", "issueFamily": "Cleanup only", "outreachRelevance": "Low"},
        {"field": "domain_migration_status/cms_migration_status", "pipeline": "Core lead", "weight": "High", "issueFamily": "Migration trigger", "outreachRelevance": "High"},
    ]


def build_spreadsheet_review(source: str) -> dict[str, Any]:
    default_columns = parse_ts_array(source, "defaultVisibleColumns")
    auto_sf_columns = parse_ts_array(source, "requiredScreamingFrog")
    return {
        "defaultVisibleColumns": default_columns,
        "autoScreamingFrogColumns": auto_sf_columns,
        "promote": [
            "sf_title_optimization",
            "sf_collection_intro",
            "sf_issue_signals",
            "sf_strengths",
            "sf_heading_health",
        ],
        "keep": [
            "sf_status",
            "sf_score",
            "sf_primary_issue",
            "sf_homepage_status",
            "sf_internal_errors",
            "sf_checked",
        ],
        "demote": [
            "sf_config",
            "sf_quality",
            "sf_pages_crawled",
            "site_status_code",
            "site_final_url",
        ],
        "notes": [
            "Worksheet currently exposes many diagnostics well, but default visibility still underplays collection title and collection intro quality.",
            "Site status fields are useful for cleanup but weaker as primary spreadsheet signals for outreach segmentation.",
            "SE deltas are valuable, but outcome flags may be too secondary or verbose compared with change columns.",
        ],
    }


def build_findings(signal_inventory: list[dict[str, Any]], spreadsheet_review: dict[str, Any], integrity_payload: dict[str, Any]) -> list[dict[str, str]]:
    findings: list[dict[str, str]] = []
    sf_inventory = next(item for item in signal_inventory if item["pipeline"] == "Screaming Frog")
    primary_families = sf_inventory["primaryIssueFamilies"]
    top_family, top_count = next(iter(primary_families.items()), ("", 0))
    sf_snapshot_rows = int(sf_inventory["snapshotRows"] or 0)
    if sf_snapshot_rows and top_family and top_count / sf_snapshot_rows >= 0.45:
        findings.append(
            {
                "priority": "P1",
                "title": "Screaming Frog primary issue families look over-dominant",
                "detail": f"`{top_family}` appears on {top_count}/{sf_snapshot_rows} saved audits, which risks flattening more useful collection/title/heading signals into one generic outreach story.",
            }
        )
    intro_statuses = sf_inventory["collectionIntroStatuses"]
    missing_intro_count = int(intro_statuses.get("missing_intro", 0))
    if sf_snapshot_rows and missing_intro_count / sf_snapshot_rows >= 0.4:
        findings.append(
            {
                "priority": "P1",
                "title": "Collection intro extraction likely still mixes real gaps with extraction misses",
                "detail": f"`missing_intro` appears on {missing_intro_count}/{sf_snapshot_rows} saved audits, so extraction quality should be reviewed before treating the signal as universally strong.",
            }
        )
    if "sf_title_optimization" not in spreadsheet_review["autoScreamingFrogColumns"]:
        findings.append(
            {
                "priority": "P1",
                "title": "CMS-specific collection title signals are still underrepresented in the worksheet",
                "detail": "The sheet auto-shows many Screaming Frog diagnostics, but it does not currently auto-promote collection title optimisation, which is one of the strongest direct outreach signals.",
            }
        )
    site_inventory = next(item for item in signal_inventory if item["pipeline"] == "Site status")
    site_categories = site_inventory["statusCategoryCounts"]
    if site_categories and len(site_categories) <= 3:
        findings.append(
            {
                "priority": "P2",
                "title": "Site status appears useful mainly for cleanup, not segmentation",
                "detail": "The current stored site-status categories are narrow enough that they likely work better as exclusions and hygiene checks than as first-class cold-email buckets.",
            }
        )
    if integrity_payload.get("exitCode", 1) != 0:
        findings.append(
            {
                "priority": "P1",
                "title": "Existing integrity harness still reports parity failures",
                "detail": integrity_payload.get("stderr") or "The integrity harness did not pass cleanly; parity findings should be reviewed before trusting downstream segmentation.",
            }
        )
    else:
        findings.append(
            {
                "priority": "P2",
                "title": "Current parity harness is API-strong but enrichment-light",
                "detail": "The existing integrity cases cover worksheet/export/analytics/timeline/preset parity well, but they do not yet validate Screaming Frog, site status, or deeper worksheet usefulness decisions with the same rigor.",
            }
        )
    return findings


def build_roadmap() -> list[str]:
    return [
        "Extend the existing integrity harness with enrichment-specific golden cases for Screaming Frog, site status, and SE Ranking display parity.",
        "Add a crawl-quality audit layer that checks seed selection, weak/partial/error classification, and 429-required-recrawl behavior.",
        "Refactor scoring review into an explicit matrix and rebalance over-dominant issue families before adding more outreach buckets.",
        "Promote collection title, collection intro, heading health, and compact issue/strength summaries in the worksheet default SF view.",
        "Demote or remove low-signal diagnostics from primary spreadsheet space when they do not improve segmentation or shortlist review.",
    ]


def render_markdown(payload: dict[str, Any]) -> str:
    lines: list[str] = [
        "# Full Audit and Crawl Pipeline Review",
        "",
        f"Generated: `{payload['generatedAt']}`",
        "",
        "## Summary",
        "",
        f"- Integrity harness status: `{'PASS' if payload['integrityAudit']['exitCode'] == 0 else 'FAIL'}`",
        f"- SE snapshot rows: `{payload['schemas']['rowCounts']['seranking_analysis_snapshots']}`",
        f"- Site status rows: `{payload['schemas']['rowCounts']['site_status_snapshots']}`",
        f"- Screaming Frog rows: `{payload['schemas']['rowCounts']['screamingfrog_audit_snapshots']}`",
        "",
        "## Pipeline Contracts",
        "",
    ]
    for item in payload["pipelineContracts"]:
        lines.extend(
            [
                f"### {item['pipeline']}",
                f"- Discovery: {item['discovery']}",
                f"- Extraction: {', '.join(item['extraction'])}",
                f"- Scoring: {item['scoring']}",
                f"- Presentation: {item['presentation']}",
                f"- Segmentation: {item['segmentation']}",
                "",
            ]
        )

    lines.extend(["## Current Findings", ""])
    for finding in payload["findings"]:
        lines.append(f"- `{finding['priority']}` {finding['title']}: {finding['detail']}")
    lines.append("")

    lines.extend(["## Crawl-Quality Matrix", ""])
    for item in payload["signalInventory"]:
        lines.append(f"### {item['pipeline']}")
        for key, value in item.items():
            if key == "pipeline":
                continue
            lines.append(f"- {key}: `{json.dumps(value, ensure_ascii=True)}`")
        lines.append("")

    lines.extend(["## Scoring Review", ""])
    for row in payload["scoringMatrix"]:
        lines.append(
            f"- `{row['field']}` ({row['pipeline']}): weight `{row['weight']}`, issue family `{row['issueFamily']}`, outreach relevance `{row['outreachRelevance']}`"
        )
    lines.append("")

    lines.extend(
        [
            "## Spreadsheet Review",
            "",
            f"- Default visible columns: `{', '.join(payload['spreadsheetReview']['defaultVisibleColumns'])}`",
            f"- Auto Screaming Frog columns: `{', '.join(payload['spreadsheetReview']['autoScreamingFrogColumns'])}`",
            f"- Promote: `{', '.join(payload['spreadsheetReview']['promote'])}`",
            f"- Keep: `{', '.join(payload['spreadsheetReview']['keep'])}`",
            f"- Demote: `{', '.join(payload['spreadsheetReview']['demote'])}`",
            "",
            "## Implementation Roadmap",
            "",
        ]
    )
    for step in payload["roadmap"]:
        lines.append(f"- {step}")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    integrity_payload = run_integrity_harness()
    frontend_source = read_text(FRONTEND_APP)

    with sqlite3.connect(STATE_DB) as connection:
        schemas = {
            "tables": {
                "seranking_analysis_snapshots": table_columns(connection, "seranking_analysis_snapshots"),
                "site_status_snapshots": table_columns(connection, "site_status_snapshots"),
                "screamingfrog_audit_snapshots": table_columns(connection, "screamingfrog_audit_snapshots"),
                "export_tray_items": table_columns(connection, "export_tray_items"),
            },
            "rowCounts": {
                "seranking_analysis_snapshots": fetch_single_value(connection, "select count(*) from seranking_analysis_snapshots"),
                "site_status_snapshots": fetch_single_value(connection, "select count(*) from site_status_snapshots"),
                "screamingfrog_audit_snapshots": fetch_single_value(connection, "select count(*) from screamingfrog_audit_snapshots"),
                "export_tray_items": fetch_single_value(connection, "select count(*) from export_tray_items"),
            },
        }
        signal_inventory = build_signal_inventory(connection)

    spreadsheet_review = build_spreadsheet_review(frontend_source)
    payload = {
        "generatedAt": now_iso(),
        "integrityAudit": integrity_payload,
        "schemas": schemas,
        "pipelineContracts": build_pipeline_contract(),
        "signalInventory": signal_inventory,
        "scoringMatrix": build_scoring_matrix(),
        "spreadsheetReview": spreadsheet_review,
        "findings": build_findings(signal_inventory, spreadsheet_review, integrity_payload),
        "roadmap": build_roadmap(),
    }

    OUTPUT_JSON.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    OUTPUT_MARKDOWN.write_text(render_markdown(payload), encoding="utf-8")
    print(f"Wrote {OUTPUT_MARKDOWN}")
    print(f"Wrote {OUTPUT_JSON}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
