from __future__ import annotations

import csv
import io
import json
import os
import re
import socket
import sqlite3
import ssl
import subprocess
import threading
import time
import uuid
from collections import Counter, defaultdict
from datetime import UTC, date, datetime
from html import escape, unescape
from html.parser import HTMLParser
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin, urlparse, urlunparse
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup
from bs4.element import Tag
from fastapi import FastAPI, HTTPException, Query, Request as FastAPIRequest
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, StreamingResponse
from pydantic import BaseModel, Field

from backend.builtin_presets import BUILTIN_PRESETS, DEFAULT_VISIBLE_COLUMNS


APP_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = Path(os.getenv("DOMAIN_DEALER_DATA_ROOT", APP_ROOT / "processed")).resolve()


def load_local_env() -> None:
    env_path = APP_ROOT / ".env"
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


load_local_env()

DATA_DB_PATH = DATA_ROOT / "builtwith.db"
SUMMARY_PATH = DATA_ROOT / "summary.json"
FILTER_OPTIONS_PATH = DATA_ROOT / "filter_options.json"
STATE_DB_PATH = DATA_ROOT / "lead_console_state.db"

DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 1000
DEFAULT_SORT_BY = "total_score"
DEFAULT_SORT_DIRECTION = "desc"

DOMAIN_CONFIDENCE_BANDS = ["High", "Medium", "Low"]
DOMAIN_FINGERPRINT_STRENGTHS = ["Strong", "Moderate", "Weak", "None"]
DOMAIN_TLD_RELATIONSHIPS = ["same_tld", "cross_tld", "unknown"]
CMS_CONFIDENCE_LEVELS = ["high", "medium", "low"]
DOMAIN_MIGRATION_STATUSES = ["confirmed", "probable", "network", "weak", "none"]
CMS_MIGRATION_STATUSES = ["confirmed", "possible", "historic", "overlap", "removed_only", "none"]
SERANKING_ANALYSIS_TYPES = ["cms_migration", "domain_migration", "manual_comparison"]
SERANKING_OUTCOME_FLAGS = [
    "traffic_up",
    "traffic_down",
    "traffic_flat",
    "traffic_up_20_plus",
    "traffic_down_20_plus",
    "traffic_up_50_plus",
    "keywords_up",
    "keywords_down",
    "keywords_flat",
    "keywords_up_20_plus",
    "keywords_down_20_plus",
    "keywords_up_50_plus",
]
SERANKING_STATUSES = ["success", "partial", "error"]
SITE_STATUS_CATEGORIES = ["ok", "redirect", "not_found", "server_error", "blocked", "timeout", "dns_error", "ssl_error", "other_error"]
SCREAMINGFROG_AUDIT_STATUSES = ["success", "partial", "error"]
SCREAMINGFROG_HOMEPAGE_STATUSES = ["ok", "redirect", "client_error", "server_error", "blocked", "other"]
SCREAMINGFROG_TITLE_FLAGS = ["missing", "duplicate", "too_long", "too_short"]
SCREAMINGFROG_META_FLAGS = ["missing", "duplicate", "too_long", "too_short"]
SCREAMINGFROG_CANONICAL_FLAGS = ["missing", "non_indexable", "inconsistent"]
SCREAMINGFROG_CRAWL_MODES = ["bounded_audit", "deep_audit"]
SERANKING_API_BASE = "https://api.seranking.com/v1"
SERANKING_API_KEY = os.getenv("SERANKING_API_KEY", "")
SERANKING_SOURCE_MAP = {"AU": "au", "NZ": "nz", "SG": "sg"}
SCREAMINGFROG_LAUNCHER_ENV = os.getenv("SCREAMING_FROG_LAUNCHER", "").strip()
SCREAMINGFROG_BOUNDED_CONFIG_ENV = os.getenv("SCREAMING_FROG_BOUNDED_CONFIG", "").strip()
SCREAMINGFROG_DEEP_CONFIG_ENV = os.getenv("SCREAMING_FROG_DEEP_CONFIG", "").strip()
SCREAMINGFROG_RUNS_ROOT = DATA_ROOT / "screamingfrog_runs"
SCREAMINGFROG_CONFIG_ROOT = APP_ROOT / "screamingfrog_configs"
SCREAMINGFROG_COLLECTION_INTELLIGENCE_ROOT = APP_ROOT / "screamingfrog_collection_intelligence"
SCREAMINGFROG_EXPORT_TABS = [
    "Internal:All",
    "Page Titles:All",
    "Meta Description:All",
    "H1:All",
    "Canonicals:All",
    "Directives:All",
    "Structured Data:All",
    "Response Codes:Internal Redirection (3xx)",
    "Response Codes:Internal Client Error (4xx)",
    "Response Codes:Internal Server Error (5xx)",
]
SCREAMINGFROG_PLATFORM_FAMILIES = ["shopify", "woocommerce", "bigcommerce", "magento", "neto", "generic"]
SCREAMINGFROG_PLATFORM_LABELS = {
    "shopify": "Shopify",
    "woocommerce": "WooCommerce",
    "bigcommerce": "BigCommerce",
    "magento": "Magento / Adobe Commerce",
    "neto": "Neto / Maropost",
    "generic": "Generic fallback",
}
SCREAMINGFROG_JOB_STATUSES = ["queued", "discovering", "running", "success", "partial", "error"]
SCREAMINGFROG_RESULT_QUALITIES = ["useful", "weak", "partial", "error"]
SCREAMINGFROG_PLATFORM_PRIORITY = [
    ("shopify", "shopify"),
    ("bigcommerce", "bigcommerce"),
    ("adobe_commerce", "magento"),
    ("magento", "magento"),
    ("maropost_commerce_cloud", "neto"),
    ("neto", "neto"),
    ("woocommerce_checkout", "woocommerce"),
    ("woocommerce", "woocommerce"),
]

STATE_DB_INIT_LOCK = threading.Lock()
STATE_DB_INITIALIZED = False
APP_STARTUP_TIME = ""
SCREAMINGFROG_WORKER_LOCK = threading.Lock()
SCREAMINGFROG_WORKER_RUNNING = False
SCREAMINGFROG_WORKER_THREAD: threading.Thread | None = None
SCREAMINGFROG_ACTIVE_PROCESS_LOCK = threading.Lock()
SCREAMINGFROG_ACTIVE_PROCESS: subprocess.Popen[str] | None = None
SCREAMINGFROG_ACTIVE_BATCH_ID = ""
SCREAMINGFROG_ACTIVE_JOB_ID = ""
SCREAMINGFROG_STOP_REQUESTED_BATCHES: set[str] = set()


def sqlite_connect(path: Path, *, timeout: int = 30, readonly: bool = False) -> sqlite3.Connection:
    if readonly:
        connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=timeout)
    else:
        connection = sqlite3.connect(path, timeout=timeout)
    connection.row_factory = sqlite3.Row
    connection.execute("pragma busy_timeout = 30000")
    if not readonly:
        connection.execute("pragma journal_mode = wal")
        connection.execute("pragma synchronous = normal")
    return connection


def with_sqlite_retry(operation, *, attempts: int = 5, delay_seconds: float = 0.25):
    last_error: sqlite3.OperationalError | None = None
    for attempt in range(attempts):
        try:
            return operation()
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower():
                raise
            last_error = exc
            time.sleep(delay_seconds * (attempt + 1))
    if last_error is not None:
        raise last_error

SORT_MAP = {
    "company": "lower(coalesce(leads.company, ''))",
    "root_domain": "lower(leads.root_domain)",
    "country": "leads.country",
    "vertical": "lower(coalesce(leads.vertical, ''))",
    "priority_tier": "case leads.priority_tier when 'A' then 1 when 'B' then 2 when 'C' then 3 else 4 end",
    "total_score": "cast(coalesce(leads.total_score, 0) as integer)",
    "technology_spend": "cast(coalesce(leads.technology_spend, 0) as integer)",
    "contact_score": "cast(coalesce(leads.contact_score, 0) as integer)",
    "bucket_count": """
        case
            when leads.sales_buckets = '' then 0
            else 1 + length(leads.sales_buckets) - length(replace(leads.sales_buckets, '|', ''))
        end
    """,
    "matched_first_detected": "coalesce(matched_first_detected, '')",
    "matched_last_found": "coalesce(matched_last_found, '')",
    "domain_migration_estimated_date": "coalesce(domain_migration_estimated_date, '')",
    "domain_migration_confidence_score": "cast(coalesce(domain_migration_confidence_score, 0) as integer)",
    "domain_migration_status": """
        case coalesce(domain_migration_status, 'none')
            when 'confirmed' then 4
            when 'probable' then 3
            when 'network' then 2
            when 'weak' then 1
            else 0
        end
    """,
    "domain_fingerprint_strength": """
        case coalesce(domain_fingerprint_strength, '')
            when 'Strong' then 4
            when 'Moderate' then 3
            when 'Weak' then 2
            when 'None' then 1
            else 0
        end
    """,
    "cms_migration_confidence": """
        case lower(coalesce(cms_migration_confidence, ''))
            when 'high' then 3
            when 'medium' then 2
            when 'low' then 1
            else 0
        end
    """,
    "cms_migration_status": """
        case lower(coalesce(cms_migration_status, 'none'))
            when 'confirmed' then 5
            when 'possible' then 4
            when 'overlap' then 3
            when 'historic' then 2
            when 'removed_only' then 1
            else 0
        end
    """,
    "cms_migration_likely_date": "coalesce(cms_migration_likely_date, '')",
    "se_ranking_traffic_delta_percent": "cast(coalesce(se_ranking_traffic_delta_percent, 0) as real)",
    "se_ranking_keywords_delta_percent": "cast(coalesce(se_ranking_keywords_delta_percent, 0) as real)",
    "se_ranking_checked_at": "coalesce(se_ranking_checked_at, '')",
    "site_status_checked_at": "coalesce(site_status.checked_at, '')",
    "site_status_code": "cast(coalesce(site_status.status_code, 0) as integer)",
    "screamingfrog_checked_at": "coalesce(screamingfrog.checked_at, '')",
    "screamingfrog_pages_crawled": "cast(coalesce(screamingfrog.pages_crawled, 0) as integer)",
    "screamingfrog_opportunity_score": "cast(coalesce(screamingfrog.sf_opportunity_score, 0) as integer)",
}


class PresetPayload(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    filters: dict[str, Any]
    sort: dict[str, Any]
    visible_columns: list[str] = Field(default_factory=lambda: DEFAULT_VISIBLE_COLUMNS.copy())


class TrayMutationPayload(BaseModel):
    root_domains: list[str] = Field(default_factory=list)


class SeRankingRunPayload(BaseModel):
    analysis_type: str = Field(pattern="^(cms_migration|domain_migration)$")
    confirm: bool = False
    filters: dict[str, Any] = Field(default_factory=dict)
    use_filtered_view: bool = False


class SeRankingRefreshPayload(BaseModel):
    analysis_type: str = Field(pattern="^(cms_migration|domain_migration)$")
    filters: dict[str, Any] = Field(default_factory=dict)
    use_filtered_view: bool = False


class SeRankingManualPayload(BaseModel):
    first_month: str
    second_month: str
    root_domains: list[str] = Field(default_factory=list)
    use_selected_tray: bool = True


class SiteStatusRunPayload(BaseModel):
    confirm: bool = False


class ScreamingFrogRunPayload(BaseModel):
    crawl_mode: str = Field(default="bounded_audit", pattern="^(bounded_audit|deep_audit)$")
    confirm: bool = False


class LinkExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        for key, value in attrs:
            if key.lower() == "href" and value:
                self.links.append(value)
                return


app = FastAPI(title="BuiltWith Lead Console", version="0.2.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


@app.on_event("startup")
def app_startup() -> None:
    global APP_STARTUP_TIME
    ensure_state_db()
    APP_STARTUP_TIME = now_iso()


@app.on_event("shutdown")
def app_shutdown() -> None:
    global SCREAMINGFROG_ACTIVE_BATCH_ID, SCREAMINGFROG_ACTIVE_JOB_ID, SCREAMINGFROG_ACTIVE_PROCESS
    global SCREAMINGFROG_STOP_REQUESTED_BATCHES, SCREAMINGFROG_WORKER_RUNNING
    if SCREAMINGFROG_ACTIVE_BATCH_ID:
        SCREAMINGFROG_STOP_REQUESTED_BATCHES.add(SCREAMINGFROG_ACTIVE_BATCH_ID)
    with SCREAMINGFROG_ACTIVE_PROCESS_LOCK:
        if SCREAMINGFROG_ACTIVE_PROCESS is not None:
            try:
                SCREAMINGFROG_ACTIVE_PROCESS.terminate()
            except Exception:
                pass
        SCREAMINGFROG_ACTIVE_PROCESS = None
        SCREAMINGFROG_ACTIVE_BATCH_ID = ""
        SCREAMINGFROG_ACTIVE_JOB_ID = ""
    with SCREAMINGFROG_WORKER_LOCK:
        SCREAMINGFROG_WORKER_RUNNING = False


def health_snapshot() -> dict[str, Any]:
    state_db_ready = False
    data_db_ready = False
    lead_query_ready = False
    error_message = ""

    try:
        ensure_state_db()
        state_db_ready = True
        with get_connection() as connection:
            connection.execute("select 1").fetchone()
            data_db_ready = True
            connection.execute("select root_domain from leads limit 1").fetchone()
            lead_query_ready = True
    except Exception as exc:
        error_message = str(exc)

    with SCREAMINGFROG_WORKER_LOCK:
        worker_alive = bool(SCREAMINGFROG_WORKER_THREAD and SCREAMINGFROG_WORKER_THREAD.is_alive())
    with SCREAMINGFROG_ACTIVE_PROCESS_LOCK:
        active_batch_id = SCREAMINGFROG_ACTIVE_BATCH_ID
        active_job_id = SCREAMINGFROG_ACTIVE_JOB_ID
        active_process_running = bool(
            SCREAMINGFROG_ACTIVE_PROCESS is not None and SCREAMINGFROG_ACTIVE_PROCESS.poll() is None
        )

    status = "ok" if state_db_ready and data_db_ready and lead_query_ready else "degraded"
    return {
        "status": status,
        "started_at": APP_STARTUP_TIME,
        "state_db_ready": state_db_ready,
        "data_db_ready": data_db_ready,
        "lead_query_ready": lead_query_ready,
        "worker_running": worker_alive,
        "active_batch_id": active_batch_id,
        "active_job_id": active_job_id,
        "active_process_running": active_process_running,
        "error": error_message,
    }


def split_pipe(value: str | None) -> list[str]:
    if not value:
        return []
    items: list[str] = []
    seen: set[str] = set()
    for item in value.split("|"):
        normalized = item.strip()
        if normalized and normalized not in seen:
            items.append(normalized)
            seen.add(normalized)
    return items


def humanize_token(value: str | None) -> str:
    raw = (value or "").strip()
    if not raw:
        return "Unknown"
    return " ".join(part.capitalize() if part.lower() not in {"api", "cms", "crm", "h1", "sf"} else part.upper() for part in raw.replace("|", " ").replace("_", " ").replace("-", " ").split())


def normalized_pipe_values(value: str | None) -> list[str]:
    return split_pipe(value)


LEAD_FILTER_DEFAULTS: dict[str, Any] = {
    "search": "",
    "exact_domain": "",
    "countries": [],
    "tiers": [],
    "current_platforms": [],
    "recent_platforms": [],
    "removed_platforms": [],
    "verticals": [],
    "sales_buckets": [],
    "live_sites_only": False,
    "timeline_platforms": [],
    "timeline_event_types": [],
    "timeline_date_field": "first_seen",
    "timeline_seen_from": "",
    "timeline_seen_to": "",
    "cms_migration_from": "",
    "cms_migration_to": "",
    "cms_unchanged_years": None,
    "domain_migration_from": "",
    "domain_migration_to": "",
    "migration_timing_operator": "and",
    "started_from": "",
    "started_to": "",
    "migration_only": False,
    "has_domain_migration": False,
    "has_cms_migration": False,
    "domain_migration_statuses": [],
    "domain_confidence_bands": [],
    "domain_fingerprint_strengths": [],
    "domain_tld_relationships": [],
    "cms_migration_statuses": [],
    "cms_confidence_levels": [],
    "has_contact": False,
    "has_marketing": False,
    "has_crm": False,
    "has_payments": False,
    "marketing_platforms": [],
    "crm_platforms": [],
    "payment_platforms": [],
    "hosting_providers": [],
    "agencies": [],
    "ai_tools": [],
    "compliance_flags": [],
    "min_social": None,
    "min_revenue": None,
    "min_employees": None,
    "min_sku": None,
    "min_technology_spend": None,
    "selected_only": False,
    "has_seranking_analysis": False,
    "seranking_analysis_types": [],
    "seranking_outcome_flags": [],
    "has_site_status_check": False,
    "site_status_categories": [],
    "has_screamingfrog_audit": False,
    "screamingfrog_statuses": [],
    "screamingfrog_homepage_statuses": [],
    "screamingfrog_title_flags": [],
    "screamingfrog_meta_flags": [],
    "screamingfrog_canonical_flags": [],
    "has_screamingfrog_internal_errors": False,
    "has_screamingfrog_location_pages": False,
    "has_screamingfrog_service_pages": False,
}


def clone_filter_default(value: Any) -> Any:
    return value.copy() if isinstance(value, list) else value


def normalize_lead_filters(raw: Mapping[str, Any] | None = None) -> dict[str, Any]:
    filters = {key: clone_filter_default(value) for key, value in LEAD_FILTER_DEFAULTS.items()}
    if raw:
        for key, value in raw.items():
            if key not in LEAD_FILTER_DEFAULTS or value is None:
                continue
            default = LEAD_FILTER_DEFAULTS[key]
            if isinstance(default, list):
                filters[key] = [item for item in value if item not in (None, "")]
            else:
                filters[key] = value

    filters["search"] = (filters["search"] or "").strip()
    filters["exact_domain"] = (filters["exact_domain"] or "").strip()
    filters["timeline_date_field"] = normalize_timeline_date_field(filters["timeline_date_field"])
    filters["migration_timing_operator"] = normalize_migration_timing_operator(filters["migration_timing_operator"])
    filters["timeline_seen_from"] = filters["timeline_seen_from"] or filters["started_from"] or ""
    filters["timeline_seen_to"] = filters["timeline_seen_to"] or filters["started_to"] or ""
    filters.pop("started_from", None)
    filters.pop("started_to", None)
    return filters


def filters_have_scope(filters: Mapping[str, Any]) -> bool:
    normalized = normalize_lead_filters(filters)
    for key, default in LEAD_FILTER_DEFAULTS.items():
        if key in {"started_from", "started_to"}:
            continue
        value = normalized.get(key)
        if isinstance(default, list):
            if value:
                return True
            continue
        if default is None:
            if value is not None:
                return True
            continue
        if value not in (default, "", None, False):
            return True
    return False


def distinct_pipe_values(connection: sqlite3.Connection, column: str) -> list[str]:
    values: set[str] = set()
    for (raw_value,) in connection.execute(f"select {column} from leads where coalesce({column}, '') != ''"):
        values.update(normalized_pipe_values(raw_value))
    return sorted(values, key=str.lower)


def collect_distinct_pipe_values_from_rows(rows: list[sqlite3.Row], column: str) -> list[str]:
    values: set[str] = set()
    for row in rows:
        values.update(normalized_pipe_values(row[column]))
    return sorted(values, key=str.lower)


@lru_cache(maxsize=1)
def dynamic_filter_options() -> dict[str, list[str]]:
    connection = get_connection()
    try:
        return {
            "marketingPlatforms": distinct_pipe_values(connection, "marketing_platforms"),
            "crmPlatforms": distinct_pipe_values(connection, "crm_platforms"),
            "paymentPlatforms": distinct_pipe_values(connection, "payment_platforms"),
            "hostingProviders": distinct_pipe_values(connection, "hosting_providers"),
            "agencies": distinct_pipe_values(connection, "agencies"),
            "aiTools": distinct_pipe_values(connection, "ai_tools"),
            "complianceFlags": distinct_pipe_values(connection, "compliance_flags"),
        }
    finally:
        connection.close()


def scoped_dynamic_filter_options(filters: dict[str, Any]) -> dict[str, list[str]]:
    normalized_filters = normalize_lead_filters(filters)
    effective_timeline_seen_from = normalized_filters["timeline_seen_from"]
    effective_timeline_seen_to = normalized_filters["timeline_seen_to"]
    effective_timeline_date_field = normalized_filters["timeline_date_field"]
    effective_migration_timing_operator = normalized_filters["migration_timing_operator"]

    connection = get_connection()
    try:
        migration_join_sql, _migration_select_sql = build_migration_join_and_select_sql()
        seranking_join_sql, _seranking_select_sql = build_seranking_join_and_select_sql()
        site_status_join_sql, _site_status_select_sql = build_site_status_join_and_select_sql()
        screamingfrog_join_sql, _screamingfrog_select_sql = build_screamingfrog_join_and_select_sql()
        scoped_filters = normalized_filters
        where, params = build_lead_filters(scoped_filters, apply_timeline_match=not bool(scoped_filters["timeline_platforms"]))
        base_join_sql = filter_base_joins(scoped_filters)
        timeline_join_sql, _matched_select_sql = build_timeline_join_and_select_sql(
            scoped_filters["timeline_platforms"],
            scoped_filters["timeline_event_types"],
            effective_timeline_date_field,
            effective_timeline_seen_from,
            effective_timeline_seen_to,
            params,
        )
        rows = connection.execute(
            f"""
            select
                leads.marketing_platforms,
                leads.crm_platforms,
                leads.payment_platforms,
                leads.hosting_providers,
                leads.agencies,
                leads.ai_tools,
                leads.compliance_flags
            from leads
            {base_join_sql}
            {migration_join_sql}
            {seranking_join_sql}
            {site_status_join_sql}
            {screamingfrog_join_sql}
            {timeline_join_sql}
            where {where}
            """,
            params,
        ).fetchall()
        return {
            "marketingPlatforms": collect_distinct_pipe_values_from_rows(rows, "marketing_platforms"),
            "crmPlatforms": collect_distinct_pipe_values_from_rows(rows, "crm_platforms"),
            "paymentPlatforms": collect_distinct_pipe_values_from_rows(rows, "payment_platforms"),
            "hostingProviders": collect_distinct_pipe_values_from_rows(rows, "hosting_providers"),
            "agencies": collect_distinct_pipe_values_from_rows(rows, "agencies"),
            "aiTools": collect_distinct_pipe_values_from_rows(rows, "ai_tools"),
            "complianceFlags": collect_distinct_pipe_values_from_rows(rows, "compliance_flags"),
        }
    finally:
        connection.close()


def json_text(value: dict[str, Any] | list[Any]) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def decode_json_text(value: str | None) -> Any:
    if not value:
        return None
    return json.loads(value)


def extract_domain_tld(domain: str | None) -> str:
    if not domain:
        return ""
    host = domain.strip().lower().strip(".")
    host = host.split("/")[0]
    if "." not in host:
        return ""
    parts = [part for part in host.split(".") if part]
    if len(parts) >= 2 and parts[-1] in {"au", "nz", "uk", "sg"} and len(parts[-2]) <= 3:
        return ".".join(parts[-2:])
    return parts[-1]


def compute_domain_tld_relationship(current_domain: str | None, old_domain: str | None) -> str:
    if not old_domain:
        return "unknown"
    current_tld = extract_domain_tld(current_domain)
    old_tld = extract_domain_tld(old_domain)
    if not current_tld or not old_tld:
        return "unknown"
    if current_tld == old_tld:
        return "same_tld"
    return "cross_tld"


def midpoint_iso_date(first_date: str | None, second_date: str | None) -> str:
    if not first_date and not second_date:
        return ""
    if not first_date:
        return second_date or ""
    if not second_date:
        return first_date or ""
    try:
        first_ts = datetime.fromisoformat(first_date).timestamp()
        second_ts = datetime.fromisoformat(second_date).timestamp()
    except ValueError:
        return second_date or first_date or ""
    midpoint = datetime.fromtimestamp((first_ts + second_ts) / 2, tz=UTC)
    return midpoint.date().isoformat()


def normalize_timeline_date_field(value: str | None) -> str:
    return "last_seen" if (value or "").lower() == "last_seen" else "first_seen"


def normalize_migration_timing_operator(value: str | None) -> str:
    return "or" if (value or "").lower() == "or" else "and"


def normalize_seranking_analysis_type(value: str | None) -> str:
    normalized = (value or "").lower()
    if normalized == "domain_migration":
        return "domain_migration"
    if normalized == "manual_comparison":
        return "manual_comparison"
    return "cms_migration"


def normalize_seranking_outcome_flags(values: list[str]) -> list[str]:
    return [value for value in values if value in SERANKING_OUTCOME_FLAGS]


def normalize_site_status_categories(values: list[str]) -> list[str]:
    return [value for value in values if value in SITE_STATUS_CATEGORIES]


def normalize_screamingfrog_statuses(values: list[str]) -> list[str]:
    return [value for value in values if value in SCREAMINGFROG_AUDIT_STATUSES]


def normalize_screamingfrog_homepage_statuses(values: list[str]) -> list[str]:
    return [value for value in values if value in SCREAMINGFROG_HOMEPAGE_STATUSES]


def normalize_screamingfrog_title_flags(values: list[str]) -> list[str]:
    return [value for value in values if value in SCREAMINGFROG_TITLE_FLAGS]


def normalize_screamingfrog_meta_flags(values: list[str]) -> list[str]:
    return [value for value in values if value in SCREAMINGFROG_META_FLAGS]


def normalize_screamingfrog_canonical_flags(values: list[str]) -> list[str]:
    return [value for value in values if value in SCREAMINGFROG_CANONICAL_FLAGS]


def parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).date()
    except ValueError:
        return None


def parse_month_key(value: str | None) -> str:
    if not value:
        return ""
    normalized = value.strip()
    if re.fullmatch(r"\d{4}-\d{2}", normalized):
        year, month = normalized.split("-", 1)
        if 1 <= int(month) <= 12:
            return f"{int(year):04d}-{int(month):02d}"
    return ""


def parse_bool_query_param(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


def parse_int_query_param(value: str | None) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError:
        return None


def years_ago_iso(years: int) -> str:
    today = date.today()
    try:
        return today.replace(year=today.year - years).isoformat()
    except ValueError:
        return today.replace(month=2, day=28, year=today.year - years).isoformat()


def extract_lead_query_args(request: FastAPIRequest) -> dict[str, Any]:
    query = request.query_params
    return normalize_lead_filters({
        "search": query.get("search"),
        "exact_domain": query.get("exact_domain"),
        "countries": query.getlist("countries"),
        "tiers": query.getlist("tiers"),
        "current_platforms": query.getlist("current_platforms"),
        "recent_platforms": query.getlist("recent_platforms"),
        "removed_platforms": query.getlist("removed_platforms"),
        "verticals": query.getlist("verticals"),
        "sales_buckets": query.getlist("sales_buckets"),
        "live_sites_only": parse_bool_query_param(query.get("live_sites_only")),
        "timeline_platforms": query.getlist("timeline_platforms"),
        "timeline_event_types": query.getlist("timeline_event_types"),
        "timeline_date_field": query.get("timeline_date_field") or "first_seen",
        "timeline_seen_from": query.get("timeline_seen_from"),
        "timeline_seen_to": query.get("timeline_seen_to"),
        "cms_migration_from": query.get("cms_migration_from"),
        "cms_migration_to": query.get("cms_migration_to"),
        "domain_migration_from": query.get("domain_migration_from"),
        "domain_migration_to": query.get("domain_migration_to"),
        "migration_timing_operator": query.get("migration_timing_operator") or "and",
        "cms_unchanged_years": parse_int_query_param(query.get("cms_unchanged_years")),
        "started_from": query.get("started_from"),
        "started_to": query.get("started_to"),
        "migration_only": parse_bool_query_param(query.get("migration_only")),
        "has_domain_migration": parse_bool_query_param(query.get("has_domain_migration")),
        "has_cms_migration": parse_bool_query_param(query.get("has_cms_migration")),
        "domain_migration_statuses": query.getlist("domain_migration_statuses"),
        "domain_confidence_bands": query.getlist("domain_confidence_bands"),
        "domain_fingerprint_strengths": query.getlist("domain_fingerprint_strengths"),
        "domain_tld_relationships": query.getlist("domain_tld_relationships"),
        "cms_migration_statuses": query.getlist("cms_migration_statuses"),
        "cms_confidence_levels": query.getlist("cms_confidence_levels"),
        "has_contact": parse_bool_query_param(query.get("has_contact")),
        "has_marketing": parse_bool_query_param(query.get("has_marketing")),
        "has_crm": parse_bool_query_param(query.get("has_crm")),
        "has_payments": parse_bool_query_param(query.get("has_payments")),
        "marketing_platforms": query.getlist("marketing_platforms"),
        "crm_platforms": query.getlist("crm_platforms"),
        "payment_platforms": query.getlist("payment_platforms"),
        "hosting_providers": query.getlist("hosting_providers"),
        "agencies": query.getlist("agencies"),
        "ai_tools": query.getlist("ai_tools"),
        "compliance_flags": query.getlist("compliance_flags"),
        "min_social": parse_int_query_param(query.get("min_social")),
        "min_revenue": parse_int_query_param(query.get("min_revenue")),
        "min_employees": parse_int_query_param(query.get("min_employees")),
        "min_sku": parse_int_query_param(query.get("min_sku")),
        "min_technology_spend": parse_int_query_param(query.get("min_technology_spend")),
        "selected_only": parse_bool_query_param(query.get("selected_only")),
        "has_seranking_analysis": parse_bool_query_param(query.get("has_seranking_analysis")),
        "seranking_analysis_types": query.getlist("seranking_analysis_types"),
        "seranking_outcome_flags": query.getlist("seranking_outcome_flags"),
        "has_site_status_check": parse_bool_query_param(query.get("has_site_status_check")),
        "site_status_categories": query.getlist("site_status_categories"),
        "has_screamingfrog_audit": parse_bool_query_param(query.get("has_screamingfrog_audit")),
        "screamingfrog_statuses": query.getlist("screamingfrog_statuses"),
        "screamingfrog_homepage_statuses": query.getlist("screamingfrog_homepage_statuses"),
        "screamingfrog_title_flags": query.getlist("screamingfrog_title_flags"),
        "screamingfrog_meta_flags": query.getlist("screamingfrog_meta_flags"),
        "screamingfrog_canonical_flags": query.getlist("screamingfrog_canonical_flags"),
        "has_screamingfrog_internal_errors": parse_bool_query_param(query.get("has_screamingfrog_internal_errors")),
        "has_screamingfrog_location_pages": parse_bool_query_param(query.get("has_screamingfrog_location_pages")),
        "has_screamingfrog_service_pages": parse_bool_query_param(query.get("has_screamingfrog_service_pages")),
    })


def month_start_for_date(value: date) -> date:
    return value.replace(day=1)


def shift_months(value: date, months: int) -> date:
    year = value.year + ((value.month - 1 + months) // 12)
    month = ((value.month - 1 + months) % 12) + 1
    return date(year, month, 1)


def last_full_month_start() -> date:
    today = datetime.now(UTC).date()
    return shift_months(month_start_for_date(today), -1)


def month_key(value: date) -> str:
    return f"{value.year:04d}-{value.month:02d}"


def within_last_eleven_months(value: date | None) -> bool:
    if value is None:
        return False
    earliest = shift_months(last_full_month_start(), -10)
    return month_start_for_date(value) >= earliest


def safe_percent_delta(before: int | float | None, after: int | float | None) -> float | None:
    if before in (None, "") or after in (None, ""):
        return None
    before_number = float(before)
    after_number = float(after)
    if before_number == 0:
        return None if after_number == 0 else 100.0
    return ((after_number - before_number) / before_number) * 100.0


def build_seranking_outcome_flags(
    traffic_delta_percent: float | None,
    keywords_delta_percent: float | None,
) -> list[str]:
    flags: list[str] = []

    def extend(prefix: str, value: float | None) -> None:
        if value is None:
            return
        if value >= 5:
            flags.append(f"{prefix}_up")
        elif value <= -5:
            flags.append(f"{prefix}_down")
        else:
            flags.append(f"{prefix}_flat")
        if value >= 20:
            flags.append(f"{prefix}_up_20_plus")
        if value <= -20:
            flags.append(f"{prefix}_down_20_plus")
        if value >= 50:
            flags.append(f"{prefix}_up_50_plus")

    extend("traffic", traffic_delta_percent)
    extend("keywords", keywords_delta_percent)
    return flags


def seranking_headers() -> dict[str, str]:
    if not SERANKING_API_KEY:
        raise HTTPException(status_code=500, detail="SE Ranking API key is not configured")
    return {"Authorization": f"Token {SERANKING_API_KEY}", "Accept": "application/json"}


def seranking_get_json(path: str, params: dict[str, Any]) -> Any:
    query = urlencode({key: value for key, value in params.items() if value not in (None, "")})
    request = Request(f"{SERANKING_API_BASE}{path}?{query}", headers=seranking_headers(), method="GET")
    try:
        with urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise HTTPException(status_code=502, detail=f"SE Ranking request failed: {exc.code} {body}") from exc
    except URLError as exc:
        raise HTTPException(status_code=502, detail=f"SE Ranking request failed: {exc.reason}") from exc


def seranking_history(domain: str, source: str) -> list[dict[str, Any]]:
    data = seranking_get_json(
        "/domain/overview/history",
        {"source": source, "domain": domain, "type": "organic"},
    )
    return data if isinstance(data, list) else []


def ensure_table_columns(connection: sqlite3.Connection, table_name: str, columns: dict[str, str]) -> None:
    existing = {
        row[1]
        for row in connection.execute(f"pragma table_info({table_name})").fetchall()
    }
    for column_name, definition in columns.items():
        if column_name not in existing:
            connection.execute(f"alter table {table_name} add column {column_name} {definition}")


def ensure_state_db() -> None:
    global STATE_DB_INITIALIZED
    if STATE_DB_INITIALIZED:
        return

    with STATE_DB_INIT_LOCK:
        if STATE_DB_INITIALIZED:
            return

        STATE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite_connect(STATE_DB_PATH)
        try:
            with_sqlite_retry(lambda: connection.execute(
                """
                create table if not exists saved_presets (
                    id text primary key,
                    name text not null,
                    is_builtin integer not null default 0,
                    filters_json text not null,
                    sort_json text not null,
                    created_at text not null,
                    updated_at text not null
                )
                """
            ))
            with_sqlite_retry(lambda: connection.execute(
                """
                create table if not exists export_tray_items (
                    root_domain text primary key,
                    added_at text not null
                )
                """
            ))
            with_sqlite_retry(lambda: connection.execute(
                """
                create table if not exists seranking_analysis_snapshots (
                    root_domain text not null,
                    analysis_type text not null,
                    analysis_mode text not null default 'migration',
                    date_mode text not null default 'migration',
                    regional_source text not null,
                    migration_likely_date text not null,
                    baseline_month text not null,
                    comparison_month text not null,
                    first_comparison_month text not null default '',
                    second_comparison_month text not null default '',
                    date_label_first text not null default '',
                    date_label_second text not null default '',
                    traffic_before integer,
                    traffic_last_month integer,
                    traffic_delta_absolute integer,
                    traffic_delta_percent real,
                    keywords_before integer,
                    keywords_last_month integer,
                    keywords_delta_absolute integer,
                    keywords_delta_percent real,
                    price_before real,
                    price_last_month real,
                    price_delta_absolute real,
                    price_delta_percent real,
                    outcome_flags text not null default '',
                    captured_at text not null,
                    status text not null,
                    error_message text not null default '',
                    primary key (root_domain, analysis_type)
                )
                """
            ))
            with_sqlite_retry(lambda: connection.execute(
                """
                create table if not exists site_status_snapshots (
                    root_domain text primary key,
                    requested_url text not null default '',
                    final_url text not null default '',
                    status_code integer,
                    status_category text not null default '',
                    redirect_count integer not null default 0,
                    checked_at text not null,
                    error_message text not null default ''
                )
                """
            ))
            with_sqlite_retry(lambda: connection.execute(
                """
                create table if not exists screamingfrog_audit_snapshots (
                    root_domain text primary key,
                    crawl_mode text not null default 'bounded_audit',
                    resolved_platform_family text not null default 'generic',
                    resolved_config_path text not null default '',
                    checked_at text not null default '',
                    status text not null default '',
                    error_message text not null default '',
                    pages_crawled integer not null default 0,
                    homepage_final_url text not null default '',
                    homepage_status_code integer,
                    homepage_status_category text not null default '',
                    homepage_indexability text not null default '',
                    homepage_title text not null default '',
                    homepage_meta_description text not null default '',
                    homepage_canonical text not null default '',
                    homepage_word_count integer not null default 0,
                    redirect_presence integer not null default 0,
                    blocked_or_noindex integer not null default 0,
                    title_issue_flags text not null default '',
                    meta_issue_flags text not null default '',
                    canonical_issue_flags text not null default '',
                    h1_issue_flags text not null default '',
                    indexable_page_count integer not null default 0,
                    internal_3xx_count integer not null default 0,
                    internal_4xx_count integer not null default 0,
                    internal_5xx_count integer not null default 0,
                    schema_page_count integer not null default 0,
                    location_page_count integer not null default 0,
                    service_page_count integer not null default 0,
                    has_internal_errors integer not null default 0,
                    export_directory text not null default ''
                )
                """
            ))
            with_sqlite_retry(lambda: connection.execute(
                """
                create table if not exists screamingfrog_jobs (
                    id text primary key,
                    batch_id text not null,
                    root_domain text not null,
                    crawl_mode text not null default 'bounded_audit',
                    resolved_platform_family text not null default 'generic',
                    status text not null default 'queued',
                    message text not null default '',
                    requested_homepage_url text not null default '',
                    final_homepage_url text not null default '',
                    redirect_detected integer not null default 0,
                    sitemap_found integer not null default 0,
                    sitemap_url text not null default '',
                    sitemap_source text not null default '',
                    seed_strategy text not null default '',
                    seed_count integer not null default 0,
                    result_quality text not null default '',
                    result_reason text not null default '',
                    started_at text not null default '',
                    completed_at text not null default '',
                    created_at text not null,
                    updated_at text not null
                )
                """
            ))
            ensure_table_columns(
                connection,
                "seranking_analysis_snapshots",
                {
                    "analysis_mode": "text not null default 'migration'",
                    "date_mode": "text not null default 'migration'",
                    "first_comparison_month": "text not null default ''",
                    "second_comparison_month": "text not null default ''",
                    "date_label_first": "text not null default ''",
                    "date_label_second": "text not null default ''",
                },
            )
            ensure_table_columns(
                connection,
                "site_status_snapshots",
                {
                    "requested_url": "text not null default ''",
                    "final_url": "text not null default ''",
                    "status_code": "integer",
                    "status_category": "text not null default ''",
                    "redirect_count": "integer not null default 0",
                    "checked_at": "text not null default ''",
                    "error_message": "text not null default ''",
                },
            )
            ensure_table_columns(
                connection,
                "screamingfrog_audit_snapshots",
                {
                    "crawl_mode": "text not null default 'bounded_audit'",
                    "resolved_platform_family": "text not null default 'generic'",
                    "resolved_config_path": "text not null default ''",
                    "checked_at": "text not null default ''",
                    "status": "text not null default ''",
                    "error_message": "text not null default ''",
                    "pages_crawled": "integer not null default 0",
                    "homepage_final_url": "text not null default ''",
                    "homepage_status_code": "integer",
                    "homepage_status_category": "text not null default ''",
                    "homepage_indexability": "text not null default ''",
                    "homepage_title": "text not null default ''",
                    "homepage_meta_description": "text not null default ''",
                    "homepage_canonical": "text not null default ''",
                    "homepage_word_count": "integer not null default 0",
                    "redirect_presence": "integer not null default 0",
                    "blocked_or_noindex": "integer not null default 0",
                    "title_issue_flags": "text not null default ''",
                    "meta_issue_flags": "text not null default ''",
                    "canonical_issue_flags": "text not null default ''",
                    "h1_issue_flags": "text not null default ''",
                    "indexable_page_count": "integer not null default 0",
                    "internal_3xx_count": "integer not null default 0",
                    "internal_4xx_count": "integer not null default 0",
                    "internal_5xx_count": "integer not null default 0",
                    "schema_page_count": "integer not null default 0",
                    "location_page_count": "integer not null default 0",
                    "service_page_count": "integer not null default 0",
                    "has_internal_errors": "integer not null default 0",
                    "export_directory": "text not null default ''",
                },
            )
            ensure_table_columns(
                connection,
                "screamingfrog_audit_snapshots",
                {
                    "requested_homepage_url": "text not null default ''",
                    "discovered_final_homepage_url": "text not null default ''",
                    "seed_strategy": "text not null default ''",
                    "seed_count": "integer not null default 0",
                    "sitemap_found": "integer not null default 0",
                    "sitemap_url": "text not null default ''",
                    "sitemap_source": "text not null default ''",
                    "result_quality": "text not null default ''",
                    "result_reason": "text not null default ''",
                    "schema_issue_flags": "text not null default ''",
                    "collection_content_issue_flags": "text not null default ''",
                    "product_metadata_issue_flags": "text not null default ''",
                    "default_title_issue_flags": "text not null default ''",
                    "homepage_issue_flags": "text not null default ''",
                    "category_page_count": "integer not null default 0",
                    "product_page_count": "integer not null default 0",
                    "sf_opportunity_score": "integer not null default 0",
                    "sf_primary_issue_family": "text not null default ''",
                    "sf_primary_issue_reason": "text not null default ''",
                    "sf_outreach_hooks": "text not null default ''",
                    "collection_detection_status": "text not null default ''",
                    "collection_detection_confidence": "integer not null default 0",
                    "collection_main_content": "text not null default ''",
                    "collection_main_content_method": "text not null default ''",
                    "collection_main_content_confidence": "integer not null default 0",
                    "collection_above_raw_text": "text not null default ''",
                    "collection_below_raw_text": "text not null default ''",
                    "collection_above_clean_text": "text not null default ''",
                    "collection_below_clean_text": "text not null default ''",
                    "collection_best_intro_text": "text not null default ''",
                    "collection_best_intro_position": "text not null default ''",
                    "collection_best_intro_confidence": "integer not null default 0",
                    "collection_best_intro_source_type": "text not null default ''",
                    "collection_intro_text": "text not null default ''",
                    "collection_intro_position": "text not null default ''",
                    "collection_intro_status": "text not null default ''",
                    "collection_intro_method": "text not null default ''",
                    "collection_intro_confidence": "integer not null default 0",
                    "collection_schema_types": "text not null default ''",
                    "collection_schema_types_method": "text not null default ''",
                    "collection_schema_types_confidence": "integer not null default 0",
                    "collection_product_count": "integer not null default 0",
                    "collection_product_count_method": "text not null default ''",
                    "collection_product_count_confidence": "integer not null default 0",
                    "collection_title_value": "text not null default ''",
                    "collection_title_method": "text not null default ''",
                    "collection_title_confidence": "integer not null default 0",
                    "collection_h1_value": "text not null default ''",
                    "collection_h1_method": "text not null default ''",
                    "collection_h1_confidence": "integer not null default 0",
                    "title_optimization_status": "text not null default ''",
                    "title_optimization_confidence": "integer not null default 0",
                    "collection_title_rule_family": "text not null default ''",
                    "collection_title_rule_match": "text not null default ''",
                    "collection_title_rule_confidence": "integer not null default 0",
                    "collection_title_site_name_match": "integer not null default 0",
                    "collection_issue_family": "text not null default ''",
                    "collection_issue_reason": "text not null default ''",
                    "heading_issue_flags": "text not null default ''",
                    "heading_outline_score": "integer not null default 0",
                    "heading_outline_summary": "text not null default ''",
                    "heading_pages_analyzed": "integer not null default 0",
                    "heading_h1_missing_count": "integer not null default 0",
                    "heading_multiple_h1_count": "integer not null default 0",
                    "heading_duplicate_h1_count": "integer not null default 0",
                    "heading_pages_with_h2_count": "integer not null default 0",
                    "heading_generic_heading_count": "integer not null default 0",
                    "heading_repeated_heading_count": "integer not null default 0",
                },
            )
            ensure_table_columns(
                connection,
                "screamingfrog_jobs",
                {
                    "batch_id": "text not null",
                    "root_domain": "text not null",
                    "crawl_mode": "text not null default 'bounded_audit'",
                    "resolved_platform_family": "text not null default 'generic'",
                    "status": "text not null default 'queued'",
                    "message": "text not null default ''",
                    "requested_homepage_url": "text not null default ''",
                    "final_homepage_url": "text not null default ''",
                    "redirect_detected": "integer not null default 0",
                    "sitemap_found": "integer not null default 0",
                    "sitemap_url": "text not null default ''",
                    "sitemap_source": "text not null default ''",
                    "seed_strategy": "text not null default ''",
                    "seed_count": "integer not null default 0",
                    "result_quality": "text not null default ''",
                    "result_reason": "text not null default ''",
                    "started_at": "text not null default ''",
                    "completed_at": "text not null default ''",
                    "created_at": "text not null default ''",
                    "updated_at": "text not null default ''",
                },
            )
            connection.execute(
                """
                create index if not exists idx_seranking_analysis_type
                on seranking_analysis_snapshots(analysis_type)
                """
            )
            connection.execute(
                """
                create index if not exists idx_seranking_status
                on seranking_analysis_snapshots(status)
                """
            )
            connection.execute(
                """
                create index if not exists idx_seranking_captured_at
                on seranking_analysis_snapshots(captured_at)
                """
            )
            connection.execute(
                """
                create index if not exists idx_site_status_category
                on site_status_snapshots(status_category)
                """
            )
            connection.execute(
                """
                create index if not exists idx_site_status_checked_at
                on site_status_snapshots(checked_at)
                """
            )
            connection.execute(
                """
                create index if not exists idx_screamingfrog_status
                on screamingfrog_audit_snapshots(status)
                """
            )
            connection.execute(
                """
                create index if not exists idx_screamingfrog_checked_at
                on screamingfrog_audit_snapshots(checked_at)
                """
            )
            connection.execute(
                """
                create index if not exists idx_screamingfrog_jobs_status
                on screamingfrog_jobs(status)
                """
            )
            connection.execute(
                """
                create index if not exists idx_screamingfrog_jobs_batch
                on screamingfrog_jobs(batch_id)
                """
            )

            for preset in BUILTIN_PRESETS:
                created_at = now_iso()
                with_sqlite_retry(lambda preset=preset, created_at=created_at: connection.execute(
                    """
                    insert into saved_presets (id, name, is_builtin, filters_json, sort_json, created_at, updated_at)
                    values (?, ?, 1, ?, ?, ?, ?)
                    on conflict(id) do update set
                        name=excluded.name,
                        is_builtin=1,
                        filters_json=excluded.filters_json,
                        sort_json=excluded.sort_json,
                        updated_at=excluded.updated_at
                    """,
                    (
                        preset["id"],
                        preset["name"],
                        json_text(preset["filters"]),
                        json_text(preset["sort"]),
                        created_at,
                        created_at,
                    ),
                ))
            with_sqlite_retry(connection.commit)
            STATE_DB_INITIALIZED = True
        finally:
            connection.close()


def get_connection() -> sqlite3.Connection:
    ensure_state_db()
    connection = sqlite_connect(DATA_DB_PATH, readonly=True)
    connection.create_function("domain_tld_relationship", 2, compute_domain_tld_relationship)
    connection.execute(f"attach database '{STATE_DB_PATH.as_posix()}' as state")
    connection.execute("pragma state.busy_timeout = 30000")
    return connection


def get_state_connection() -> sqlite3.Connection:
    ensure_state_db()
    connection = sqlite_connect(STATE_DB_PATH)
    connection.execute(f"attach database '{DATA_DB_PATH.as_posix()}' as data")
    connection.execute("pragma data.busy_timeout = 30000")
    return connection


@lru_cache(maxsize=64)
def data_table_exists(table_name: str) -> bool:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table_name):
        return False
    connection = sqlite_connect(DATA_DB_PATH, readonly=True)
    try:
        row = connection.execute(
            "select 1 from sqlite_master where type = 'table' and name = ?",
            (table_name,),
        ).fetchone()
        return row is not None
    finally:
        connection.close()


def normalize_domain_search(value: str | None) -> str:
    if not value:
        return ""
    domain = value.strip().lower()
    domain = domain.removeprefix("http://").removeprefix("https://").removeprefix("www.")
    domain = domain.split("/", 1)[0].strip(".")
    return domain


def build_search_clause(search: str, params: dict[str, Any]) -> str:
    normalized = search.strip().lower()
    exact_domain = normalize_domain_search(search)
    conditions: list[str] = []

    if exact_domain:
        params["search_exact_domain"] = exact_domain
        conditions.append("lower(leads.root_domain) = :search_exact_domain")
        if "." in exact_domain:
            params["search_domain_prefix"] = f"{exact_domain}%"
            conditions.append("lower(leads.root_domain) like :search_domain_prefix")

    tokens = [token for token in re.split(r"\s+", normalized) if token]
    fts_terms = []
    for token in tokens:
        safe = re.sub(r"[^a-z0-9]", "", token)
        if safe:
            fts_terms.append(f'"{safe}"*')
    if fts_terms:
        params["search_fts"] = " AND ".join(fts_terms)
        conditions.append(
            "leads.rowid in (select rowid from leads_search where leads_search match :search_fts)"
        )
        return "(" + " or ".join(conditions) + ")"

    # Fallback for short or punctuation-heavy queries that do not produce usable FTS tokens.
    params["search_like"] = f"%{normalized}%"
    conditions.extend(
        [
            "lower(leads.root_domain) like :search_like",
            "lower(coalesce(leads.company, '')) like :search_like",
            "lower(coalesce(leads.vertical, '')) like :search_like",
        ]
    )

    return "(" + " or ".join(conditions) + ")"


def like_clause(column: str, values: list[str], params: dict[str, Any], prefix: str) -> str:
    pieces = []
    for index, value in enumerate(values):
        key = f"{prefix}_{index}"
        params[key] = f"%{value}%"
        pieces.append(f"{column} like :{key}")
    return "(" + " or ".join(pieces) + ")"


def in_clause(column: str, values: list[str], params: dict[str, Any], prefix: str) -> str:
    keys = []
    for index, value in enumerate(values):
        key = f"{prefix}_{index}"
        params[key] = value
        keys.append(f":{key}")
    return f"{column} in ({', '.join(keys)})"


def _build_lead_filters_legacy(
    search: str | None,
    exact_domain: str | None,
    countries: list[str],
    tiers: list[str],
    current_platforms: list[str],
    recent_platforms: list[str],
    removed_platforms: list[str],
    verticals: list[str],
    sales_buckets: list[str],
    live_sites_only: bool,
    timeline_platforms: list[str],
    timeline_event_types: list[str],
    timeline_date_field: str,
    timeline_seen_from: str | None,
    timeline_seen_to: str | None,
    cms_migration_from: str | None,
    cms_migration_to: str | None,
    cms_unchanged_years: int | None,
    domain_migration_from: str | None,
    domain_migration_to: str | None,
    migration_timing_operator: str,
    migration_only: bool,
    has_domain_migration: bool,
    has_cms_migration: bool,
    domain_migration_statuses: list[str],
    domain_confidence_bands: list[str],
    domain_fingerprint_strengths: list[str],
    domain_tld_relationships: list[str],
    cms_migration_statuses: list[str],
    cms_confidence_levels: list[str],
    has_contact: bool,
    has_marketing: bool,
    has_crm: bool,
    has_payments: bool,
    marketing_platforms: list[str],
    crm_platforms: list[str],
    payment_platforms: list[str],
    hosting_providers: list[str],
    agencies: list[str],
    ai_tools: list[str],
    compliance_flags: list[str],
    min_social: int | None,
    min_revenue: int | None,
    min_employees: int | None,
    min_sku: int | None,
    min_technology_spend: int | None,
    selected_only: bool,
    has_seranking_analysis: bool,
    seranking_analysis_types: list[str],
    seranking_outcome_flags: list[str],
    has_site_status_check: bool,
    site_status_categories: list[str],
    has_screamingfrog_audit: bool,
    screamingfrog_statuses: list[str],
    screamingfrog_homepage_statuses: list[str],
    screamingfrog_title_flags: list[str],
    screamingfrog_meta_flags: list[str],
    screamingfrog_canonical_flags: list[str],
    has_screamingfrog_internal_errors: bool,
    has_screamingfrog_location_pages: bool,
    has_screamingfrog_service_pages: bool,
    apply_timeline_match: bool = True,
) -> tuple[str, dict[str, Any]]:
    where = ["1=1"]
    params: dict[str, Any] = {}

    if search:
        where.append(build_search_clause(search, params))
    if exact_domain:
        params["exact_domain"] = normalize_domain_search(exact_domain)
        where.append("lower(leads.root_domain) = :exact_domain")

    if countries:
        where.append(in_clause("leads.country", countries, params, "country"))
    if tiers:
        where.append(in_clause("leads.priority_tier", tiers, params, "tier"))
    if verticals:
        where.append(in_clause("leads.vertical", verticals, params, "vertical"))
    if current_platforms:
        where.append(like_clause("leads.current_platforms", current_platforms, params, "current"))
    if recent_platforms:
        where.append(like_clause("leads.recently_added_platforms", recent_platforms, params, "recent"))
    if removed_platforms:
        where.append(like_clause("leads.removed_platforms", removed_platforms, params, "removed"))
    if sales_buckets:
        where.append(like_clause("leads.sales_buckets", sales_buckets, params, "bucket"))
    if live_sites_only:
        where.append("coalesce(trim(leads.current_platforms), '') != ''")
    if migration_only:
        where.append("leads.migration_candidate_flag = 1")
    if has_domain_migration:
        where.append("coalesce(domain_migration.best_old_domain, '') != ''")
    if has_cms_migration:
        where.append("coalesce(leads.cms_migration_status, 'none') not in ('none', 'removed_only')")
    if domain_migration_statuses:
        where.append(
            in_clause(
                "coalesce(domain_migration.domain_migration_status, 'none')",
                domain_migration_statuses,
                params,
                "domain_status",
            )
        )
    if domain_confidence_bands:
        where.append(
            in_clause(
                "coalesce(domain_migration.domain_migration_confidence_band, '')",
                domain_confidence_bands,
                params,
                "domain_confidence",
            )
        )
    if domain_fingerprint_strengths:
        where.append(
            in_clause(
                "coalesce(domain_migration.domain_fingerprint_strength, '')",
                domain_fingerprint_strengths,
                params,
                "domain_fingerprint",
            )
        )
    if domain_tld_relationships:
        where.append(
            in_clause(
                "coalesce(domain_migration.domain_tld_relationship, 'unknown')",
                domain_tld_relationships,
                params,
                "domain_tld",
            )
        )
    if cms_migration_statuses:
        where.append(
            in_clause(
                "coalesce(leads.cms_migration_status, 'none')",
                cms_migration_statuses,
                params,
                "cms_status",
            )
        )
    if cms_confidence_levels:
        where.append(
            in_clause(
                "lower(coalesce(leads.cms_migration_confidence, ''))",
                [value.lower() for value in cms_confidence_levels],
                params,
                "cms_confidence",
            )
        )
    if has_contact:
        where.append("(leads.emails != '' or leads.telephones != '' or leads.people != '')")
    if has_marketing:
        where.append("leads.marketing_platforms != ''")
    if has_crm:
        where.append("leads.crm_platforms != ''")
    if has_payments:
        where.append("leads.payment_platforms != ''")
    if marketing_platforms:
        where.append(like_clause("leads.marketing_platforms", marketing_platforms, params, "marketing_platform"))
    if crm_platforms:
        where.append(like_clause("leads.crm_platforms", crm_platforms, params, "crm_platform"))
    if payment_platforms:
        where.append(like_clause("leads.payment_platforms", payment_platforms, params, "payment_platform"))
    if hosting_providers:
        where.append(like_clause("leads.hosting_providers", hosting_providers, params, "hosting_provider"))
    if agencies:
        where.append(like_clause("leads.agencies", agencies, params, "agency"))
    if ai_tools:
        where.append(like_clause("leads.ai_tools", ai_tools, params, "ai_tool"))
    if compliance_flags:
        where.append(like_clause("leads.compliance_flags", compliance_flags, params, "compliance_flag"))
    if min_social is not None:
        params["min_social"] = min_social
        where.append("coalesce(leads.social, 0) >= :min_social")
    if min_revenue is not None:
        params["min_revenue"] = min_revenue
        where.append("coalesce(leads.sales_revenue, 0) >= :min_revenue")
    if min_employees is not None:
        params["min_employees"] = min_employees
        where.append("coalesce(leads.employees, 0) >= :min_employees")
    if min_sku is not None:
        params["min_sku"] = min_sku
        where.append("coalesce(leads.sku, 0) >= :min_sku")
    if min_technology_spend is not None:
        params["min_technology_spend"] = min_technology_spend
        where.append("coalesce(leads.technology_spend, 0) >= :min_technology_spend")
    if selected_only:
        where.append("exists (select 1 from state.export_tray_items tray where tray.root_domain = leads.root_domain)")
    if has_seranking_analysis:
        where.append("coalesce(se_ranking.analysis_type, '') != ''")
    if seranking_analysis_types:
        where.append(
            in_clause(
                "coalesce(se_ranking.analysis_type, '')",
                [normalize_seranking_analysis_type(value) for value in seranking_analysis_types],
                params,
                "se_analysis_type",
            )
        )
    if seranking_outcome_flags:
        where.append(like_clause("se_ranking.outcome_flags", normalize_seranking_outcome_flags(seranking_outcome_flags), params, "se_outcome"))
    if has_site_status_check:
        where.append("coalesce(site_status.status_category, '') != ''")
    if site_status_categories:
        where.append(
            in_clause(
                "coalesce(site_status.status_category, '')",
                normalize_site_status_categories(site_status_categories),
                params,
                "site_status_category",
            )
        )
    if has_screamingfrog_audit:
        where.append("coalesce(screamingfrog.status, '') != ''")
    if screamingfrog_statuses:
        where.append(
            in_clause(
                "coalesce(screamingfrog.status, '')",
                normalize_screamingfrog_statuses(screamingfrog_statuses),
                params,
                "sf_status",
            )
        )
    if screamingfrog_homepage_statuses:
        where.append(
            in_clause(
                "coalesce(screamingfrog.homepage_status_category, '')",
                normalize_screamingfrog_homepage_statuses(screamingfrog_homepage_statuses),
                params,
                "sf_homepage_status",
            )
        )
    if screamingfrog_title_flags:
        where.append(
            like_clause(
                "coalesce(screamingfrog.title_issue_flags, '')",
                normalize_screamingfrog_title_flags(screamingfrog_title_flags),
                params,
                "sf_title_flag",
            )
        )
    if screamingfrog_meta_flags:
        where.append(
            like_clause(
                "coalesce(screamingfrog.meta_issue_flags, '')",
                normalize_screamingfrog_meta_flags(screamingfrog_meta_flags),
                params,
                "sf_meta_flag",
            )
        )
    if screamingfrog_canonical_flags:
        where.append(
            like_clause(
                "coalesce(screamingfrog.canonical_issue_flags, '')",
                normalize_screamingfrog_canonical_flags(screamingfrog_canonical_flags),
                params,
                "sf_canonical_flag",
            )
        )
    if has_screamingfrog_internal_errors:
        where.append("coalesce(screamingfrog.has_internal_errors, 0) = 1")
    if has_screamingfrog_location_pages:
        where.append("coalesce(screamingfrog.location_page_count, 0) > 0")
    if has_screamingfrog_service_pages:
        where.append("coalesce(screamingfrog.service_page_count, 0) > 0")
    cms_timing_conditions: list[str] = []
    domain_timing_conditions: list[str] = []
    implicit_cms_migration_only = bool(cms_migration_from or cms_migration_to) and not cms_migration_statuses and not has_cms_migration
    if implicit_cms_migration_only:
        cms_timing_conditions.append("coalesce(leads.cms_migration_status, 'none') not in ('none', 'removed_only')")
    if cms_migration_from:
        params["cms_migration_from"] = cms_migration_from
        cms_timing_conditions.append("coalesce(leads.cms_migration_likely_date, '') >= :cms_migration_from")
    if cms_migration_to:
        params["cms_migration_to"] = cms_migration_to
        cms_timing_conditions.append("coalesce(leads.cms_migration_likely_date, '') <= :cms_migration_to")
    if cms_unchanged_years is not None and cms_unchanged_years > 0:
        params["cms_unchanged_cutoff"] = years_ago_iso(min(cms_unchanged_years, 50))
        where.append(
            """
            coalesce(cms_stability.oldest_current_first_detected, '') != ''
            and cms_stability.oldest_current_first_detected <= :cms_unchanged_cutoff
            and coalesce(cms_stability.newest_current_first_detected, '') <= :cms_unchanged_cutoff
            and (
                coalesce(cms_stability.latest_cms_change_date, '') = ''
                or cms_stability.latest_cms_change_date <= :cms_unchanged_cutoff
            )
            """
        )
    if domain_migration_from:
        params["domain_migration_from"] = domain_migration_from
        domain_timing_conditions.append("coalesce(domain_migration.domain_migration_estimated_date, '') >= :domain_migration_from")
    if domain_migration_to:
        params["domain_migration_to"] = domain_migration_to
        domain_timing_conditions.append("coalesce(domain_migration.domain_migration_estimated_date, '') <= :domain_migration_to")
    if cms_timing_conditions and domain_timing_conditions:
        operator = "or" if normalize_migration_timing_operator(migration_timing_operator) == "or" else "and"
        where.append(f"(({' and '.join(cms_timing_conditions)}) {operator} ({' and '.join(domain_timing_conditions)}))")
    elif cms_timing_conditions:
        where.append(f"({' and '.join(cms_timing_conditions)})")
    elif domain_timing_conditions:
        where.append(f"({' and '.join(domain_timing_conditions)})")
    if timeline_platforms and apply_timeline_match:
        timeline_clause = build_timeline_clause(
            "tt",
            timeline_platforms,
            timeline_event_types,
            timeline_date_field,
            timeline_seen_from,
            timeline_seen_to,
            params,
            "timeline",
        )
        where.append(
            f"exists (select 1 from technology_timelines tt where tt.root_domain = leads.root_domain and {timeline_clause})"
        )

    return " and ".join(where), params


def build_lead_filters(filters: Mapping[str, Any], *, apply_timeline_match: bool = True) -> tuple[str, dict[str, Any]]:
    normalized = normalize_lead_filters(filters)
    return _build_lead_filters_legacy(
        normalized["search"],
        normalized["exact_domain"],
        normalized["countries"],
        normalized["tiers"],
        normalized["current_platforms"],
        normalized["recent_platforms"],
        normalized["removed_platforms"],
        normalized["verticals"],
        normalized["sales_buckets"],
        normalized["live_sites_only"],
        normalized["timeline_platforms"],
        normalized["timeline_event_types"],
        normalized["timeline_date_field"],
        normalized["timeline_seen_from"],
        normalized["timeline_seen_to"],
        normalized["cms_migration_from"],
        normalized["cms_migration_to"],
        normalized["cms_unchanged_years"],
        normalized["domain_migration_from"],
        normalized["domain_migration_to"],
        normalized["migration_timing_operator"],
        normalized["migration_only"],
        normalized["has_domain_migration"],
        normalized["has_cms_migration"],
        normalized["domain_migration_statuses"],
        normalized["domain_confidence_bands"],
        normalized["domain_fingerprint_strengths"],
        normalized["domain_tld_relationships"],
        normalized["cms_migration_statuses"],
        normalized["cms_confidence_levels"],
        normalized["has_contact"],
        normalized["has_marketing"],
        normalized["has_crm"],
        normalized["has_payments"],
        normalized["marketing_platforms"],
        normalized["crm_platforms"],
        normalized["payment_platforms"],
        normalized["hosting_providers"],
        normalized["agencies"],
        normalized["ai_tools"],
        normalized["compliance_flags"],
        normalized["min_social"],
        normalized["min_revenue"],
        normalized["min_employees"],
        normalized["min_sku"],
        normalized["min_technology_spend"],
        normalized["selected_only"],
        normalized["has_seranking_analysis"],
        normalized["seranking_analysis_types"],
        normalized["seranking_outcome_flags"],
        normalized["has_site_status_check"],
        normalized["site_status_categories"],
        normalized["has_screamingfrog_audit"],
        normalized["screamingfrog_statuses"],
        normalized["screamingfrog_homepage_statuses"],
        normalized["screamingfrog_title_flags"],
        normalized["screamingfrog_meta_flags"],
        normalized["screamingfrog_canonical_flags"],
        normalized["has_screamingfrog_internal_errors"],
        normalized["has_screamingfrog_location_pages"],
        normalized["has_screamingfrog_service_pages"],
        apply_timeline_match=apply_timeline_match,
    )


def filter_count_joins(filters: Mapping[str, Any]) -> str:
    joins: list[str] = [filter_base_joins(filters)]
    if (
        filters.get("has_domain_migration")
        or filters.get("domain_migration_statuses")
        or filters.get("domain_confidence_bands")
        or filters.get("domain_fingerprint_strengths")
        or filters.get("domain_tld_relationships")
        or filters.get("domain_migration_from")
        or filters.get("domain_migration_to")
    ):
        joins.append(build_migration_join_and_select_sql()[0])
    if filters.get("has_seranking_analysis") or filters.get("seranking_analysis_types") or filters.get("seranking_outcome_flags"):
        joins.append(build_seranking_join_and_select_sql()[0])
    if filters.get("has_site_status_check") or filters.get("site_status_categories"):
        joins.append(build_site_status_join_and_select_sql()[0])
    if (
        filters.get("has_screamingfrog_audit")
        or filters.get("screamingfrog_statuses")
        or filters.get("screamingfrog_homepage_statuses")
        or filters.get("screamingfrog_title_flags")
        or filters.get("screamingfrog_meta_flags")
        or filters.get("screamingfrog_canonical_flags")
        or filters.get("has_screamingfrog_internal_errors")
        or filters.get("has_screamingfrog_location_pages")
        or filters.get("has_screamingfrog_service_pages")
    ):
        joins.append(build_screamingfrog_join_and_select_sql()[0])
    return "\n".join(joins)


def filter_base_joins(filters: Mapping[str, Any]) -> str:
    if filters.get("cms_unchanged_years") is not None:
        return "join cms_stability on cms_stability.root_domain = leads.root_domain"
    return ""


def normalize_sort(sort_by: str | None, sort_direction: str | None) -> tuple[str, str]:
    safe_sort_by = sort_by if sort_by in SORT_MAP else DEFAULT_SORT_BY
    safe_direction = "asc" if (sort_direction or "").lower() == "asc" else "desc"
    return safe_sort_by, safe_direction


def build_order_clause(sort_by: str | None, sort_direction: str | None, search: str | None = None) -> str:
    safe_sort_by, safe_direction = normalize_sort(sort_by, sort_direction)
    expression = SORT_MAP[safe_sort_by]
    exact_domain = normalize_domain_search(search)
    if exact_domain:
        return (
            "case when lower(leads.root_domain) = :search_exact_domain then 0 else 1 end asc, "
            f"{expression} {safe_direction}, lower(leads.root_domain) asc"
        )
    return f"{expression} {safe_direction}, lower(leads.root_domain) asc"


def build_timeline_clause(
    alias: str,
    timeline_platforms: list[str],
    timeline_event_types: list[str],
    timeline_date_field: str,
    timeline_seen_from: str | None,
    timeline_seen_to: str | None,
    params: dict[str, Any],
    prefix: str,
) -> str:
    conditions: list[str] = []
    date_column = f"{alias}.last_found" if normalize_timeline_date_field(timeline_date_field) == "last_seen" else f"{alias}.first_detected"
    if timeline_platforms:
        conditions.append(in_clause(f"{alias}.platform", timeline_platforms, params, f"{prefix}_platform"))

    if timeline_event_types:
        event_parts = []
        if "current_detected" in timeline_event_types:
            event_parts.append(f"{alias}.has_current_detected = 1")
        if "recently_added" in timeline_event_types:
            event_parts.append(f"{alias}.has_recently_added = 1")
        if "no_longer_detected" in timeline_event_types:
            event_parts.append(f"{alias}.has_removed = 1")
        conditions.append("(" + " or ".join(event_parts) + ")" if event_parts else "0 = 1")
    else:
        conditions.append("0 = 1")

    if timeline_seen_from:
        params[f"{prefix}_seen_from"] = timeline_seen_from
        conditions.append(f"coalesce({date_column}, '') >= :{prefix}_seen_from")
    if timeline_seen_to:
        params[f"{prefix}_seen_to"] = timeline_seen_to
        conditions.append(f"coalesce({date_column}, '') <= :{prefix}_seen_to")

    return " and ".join(conditions) if conditions else "1 = 1"


def build_timeline_join_and_select_sql(
    timeline_platforms: list[str],
    timeline_event_types: list[str],
    timeline_date_field: str,
    timeline_seen_from: str | None,
    timeline_seen_to: str | None,
    params: dict[str, Any],
) -> tuple[str, str]:
    if not timeline_platforms:
        return "", """
            '' as matched_first_detected,
            '' as matched_last_found,
            '' as matched_timeline_platforms
        """

    match_clause = build_timeline_clause(
        "tt",
        timeline_platforms,
        timeline_event_types,
        timeline_date_field,
        timeline_seen_from,
        timeline_seen_to,
        params,
        "matched_join",
    )
    join_sql = f"""
        join (
            select matched.root_domain,
                   min(matched.first_detected) as matched_first_detected,
                   max(matched.last_found) as matched_last_found,
                   group_concat(matched.platform, ' | ') as matched_timeline_platforms
            from (
                select distinct tt.root_domain, tt.platform, tt.first_detected, tt.last_found
                from technology_timelines tt
                where {match_clause}
                order by tt.platform
            ) matched
            group by matched.root_domain
        ) matched_timelines on matched_timelines.root_domain = leads.root_domain
    """
    select_sql = """
        coalesce(matched_timelines.matched_first_detected, '') as matched_first_detected,
        coalesce(matched_timelines.matched_last_found, '') as matched_last_found,
        coalesce(matched_timelines.matched_timeline_platforms, '') as matched_timeline_platforms
    """
    return join_sql, select_sql


def build_migration_join_and_select_sql() -> tuple[str, str]:
    if data_table_exists("domain_migration_best_match_ui"):
        join_sql = """
            left join (
                select
                    current_domain as root_domain,
                    best_old_domain,
                    domain_migration_estimated_date,
                    domain_redirect_first_seen,
                    domain_redirect_last_seen,
                    domain_migration_date_source,
                    domain_migration_status,
                    domain_migration_reason,
                    domain_migration_confidence_score,
                    domain_migration_confidence_band,
                    domain_fingerprint_strength,
                    domain_migration_candidate_count,
                    domain_shared_signals,
                    domain_shared_technologies,
                    domain_migration_notes,
                    domain_fingerprint_notes,
                    old_company,
                    old_country,
                    old_ecommerce_platforms,
                    domain_tld_relationship,
                    domain_migration_warning_flags,
                    domain_migration_evidence_flags
                from domain_migration_best_match_ui
            ) domain_migration on domain_migration.root_domain = leads.root_domain
        """
    else:
        join_sql = """
            left join (
                select
                    '' as root_domain,
                    '' as best_old_domain,
                    '' as domain_migration_estimated_date,
                    '' as domain_redirect_first_seen,
                    '' as domain_redirect_last_seen,
                    '' as domain_migration_date_source,
                    'none' as domain_migration_status,
                    '' as domain_migration_reason,
                    '' as domain_migration_confidence_score,
                    '' as domain_migration_confidence_band,
                    '' as domain_fingerprint_strength,
                    '' as domain_migration_candidate_count,
                    '' as domain_shared_signals,
                    '' as domain_shared_technologies,
                    '' as domain_migration_notes,
                    '' as domain_fingerprint_notes,
                    '' as old_company,
                    '' as old_country,
                    '' as old_ecommerce_platforms,
                    'unknown' as domain_tld_relationship,
                    '' as domain_migration_warning_flags,
                    '' as domain_migration_evidence_flags
                where 0
            ) domain_migration on domain_migration.root_domain = leads.root_domain
        """
    select_sql = """
        coalesce(domain_migration.best_old_domain, '') as best_old_domain,
        coalesce(domain_migration.domain_migration_estimated_date, '') as domain_migration_estimated_date,
        coalesce(domain_migration.domain_redirect_first_seen, '') as domain_redirect_first_seen,
        coalesce(domain_migration.domain_redirect_last_seen, '') as domain_redirect_last_seen,
        coalesce(domain_migration.domain_migration_date_source, '') as domain_migration_date_source,
        coalesce(domain_migration.domain_migration_confidence_score, '') as domain_migration_confidence_score,
        coalesce(domain_migration.domain_migration_confidence_band, '') as domain_migration_confidence_band,
        coalesce(domain_migration.domain_fingerprint_strength, '') as domain_fingerprint_strength,
        coalesce(domain_migration.domain_migration_candidate_count, '') as domain_migration_candidate_count,
        coalesce(domain_migration.domain_shared_signals, '') as domain_shared_signals,
        coalesce(domain_migration.domain_shared_technologies, '') as domain_shared_technologies,
        coalesce(domain_migration.domain_migration_notes, '') as domain_migration_notes,
        coalesce(domain_migration.domain_fingerprint_notes, '') as domain_fingerprint_notes,
        coalesce(domain_migration.domain_tld_relationship, 'unknown') as domain_tld_relationship,
        coalesce(domain_migration.domain_migration_status, 'none') as domain_migration_status,
        coalesce(domain_migration.domain_migration_reason, '') as domain_migration_reason,
        coalesce(domain_migration.domain_migration_warning_flags, '') as domain_migration_warning_flags,
        coalesce(domain_migration.domain_migration_evidence_flags, '') as domain_migration_evidence_flags,
        coalesce(leads.cms_migration_status, 'none') as cms_migration_status,
        coalesce(leads.cms_migration_confidence, 'none') as cms_migration_confidence,
        coalesce(leads.cms_migration_reason, '') as cms_migration_reason,
        coalesce(leads.cms_migration_old_platform, '') as cms_old_platform,
        coalesce(leads.cms_migration_new_platform, '') as cms_new_platform,
        coalesce(leads.cms_migration_gap_days, '') as cms_migration_gap_days,
        coalesce(leads.cms_migration_likely_date, '') as cms_migration_likely_date,
        coalesce(leads.cms_migration_first_new_seen, '') as cms_first_new_detected,
        coalesce(leads.cms_migration_last_old_seen, '') as cms_last_old_found,
        coalesce(leads.cms_migration_warning_flags, '') as cms_migration_warning_flags,
        coalesce(leads.cms_migration_evidence_flags, '') as cms_migration_evidence_flags,
        trim(coalesce(leads.cms_migration_old_platform, '') || ' -> ' || coalesce(leads.cms_migration_new_platform, '')) as cms_migration_summary
    """
    return join_sql, select_sql


def period_label(date_text: str, granularity: str) -> str:
    date_value = datetime.fromisoformat(date_text).date()
    if granularity == "week":
        iso_year, iso_week, _iso_day = date_value.isocalendar()
        return f"{iso_year}-W{iso_week:02d}"
    if granularity == "quarter":
        quarter = ((date_value.month - 1) // 3) + 1
        return f"{date_value.year}-Q{quarter}"
    return f"{date_value.year:04d}-{date_value.month:02d}"


def lead_row_to_item(row: sqlite3.Row) -> dict[str, Any]:
    data = dict(row)
    sales_buckets = split_pipe(data.get("sales_buckets"))
    bucket_reasons_raw = [reason.strip() for reason in (data.get("bucket_reasons") or "").split("||") if reason.strip()]
    trigger_score = int(data.get("trigger_score") or 0)
    total_score = int(data.get("total_score") or 0)
    sf_score = int(data.get("screamingfrog_opportunity_score") or 0)
    sf_issue_family = (data.get("screamingfrog_primary_issue_family") or "").strip()
    sf_issue_reason = (data.get("screamingfrog_primary_issue_reason") or "").strip()
    if sf_score > 0 and data.get("screamingfrog_status"):
        sf_bonus = min(6, max(1, sf_score // 10))
        trigger_score += sf_bonus
        total_score += sf_bonus
        if "screamingfrog_audited" not in sales_buckets:
            sales_buckets.append("screamingfrog_audited")
        if sf_issue_family:
            family_bucket = f"sf_{sf_issue_family}"
            if family_bucket not in sales_buckets:
                sales_buckets.append(family_bucket)
        if sf_issue_reason:
            bucket_reasons_raw.append(f"screamingfrog_signal: {sf_issue_reason}")
    return {
        **data,
        "trigger_score": trigger_score,
        "total_score": total_score,
        "matched_first_detected": data.get("matched_first_detected", ""),
        "matched_last_found": data.get("matched_last_found", ""),
        "domain_migration_estimated_date": data.get("domain_migration_estimated_date", ""),
        "domain_redirect_first_seen": data.get("domain_redirect_first_seen", ""),
        "domain_redirect_last_seen": data.get("domain_redirect_last_seen", ""),
        "domain_migration_date_source": data.get("domain_migration_date_source", ""),
        "recently_added_platforms": split_pipe(data.get("recently_added_platforms")),
        "removed_platforms": split_pipe(data.get("removed_platforms")),
        "current_platforms": split_pipe(data.get("current_platforms")),
        "current_candidate_platforms": split_pipe(data.get("current_candidate_platforms")),
        "likely_current_platforms": split_pipe(data.get("likely_current_platforms")),
        "matched_timeline_platforms": split_pipe(data.get("matched_timeline_platforms")),
        "integrity_flags": split_pipe(data.get("integrity_flags")),
        "domain_shared_signals": split_pipe(data.get("domain_shared_signals")),
        "domain_shared_technologies": split_pipe(data.get("domain_shared_technologies")),
        "domain_migration_warning_flags": split_pipe(data.get("domain_migration_warning_flags")),
        "domain_migration_evidence_flags": split_pipe(data.get("domain_migration_evidence_flags")),
        "marketing_platforms": split_pipe(data.get("marketing_platforms")),
        "payment_platforms": split_pipe(data.get("payment_platforms")),
        "crm_platforms": split_pipe(data.get("crm_platforms")),
        "hosting_providers": split_pipe(data.get("hosting_providers")),
        "agencies": split_pipe(data.get("agencies")),
        "ai_tools": split_pipe(data.get("ai_tools")),
        "compliance_flags": split_pipe(data.get("compliance_flags")),
        "sales_buckets": sales_buckets,
        "cms_migration_warning_flags": split_pipe(data.get("cms_migration_warning_flags")),
        "cms_migration_evidence_flags": split_pipe(data.get("cms_migration_evidence_flags")),
        "se_ranking_outcome_flags": split_pipe(data.get("se_ranking_outcome_flags")),
        "site_status_category": data.get("site_status_category") or "",
        "site_status_code": data.get("site_status_code") or "",
        "site_status_final_url": data.get("site_status_final_url") or "",
        "site_status_checked_at": data.get("site_status_checked_at") or "",
        "site_status_error": data.get("site_status_error") or "",
        "site_status_redirect_count": data.get("site_status_redirect_count") or 0,
        "screamingfrog_crawl_mode": data.get("screamingfrog_crawl_mode") or "",
        "screamingfrog_resolved_platform_family": data.get("screamingfrog_resolved_platform_family") or "",
        "screamingfrog_resolved_config_path": data.get("screamingfrog_resolved_config_path") or "",
        "screamingfrog_result_quality": data.get("screamingfrog_result_quality") or "",
        "screamingfrog_result_reason": data.get("screamingfrog_result_reason") or "",
        "screamingfrog_seed_strategy": data.get("screamingfrog_seed_strategy") or "",
        "screamingfrog_seed_count": data.get("screamingfrog_seed_count") or 0,
        "screamingfrog_sitemap_found": data.get("screamingfrog_sitemap_found") or 0,
        "screamingfrog_sitemap_url": unescape(data.get("screamingfrog_sitemap_url") or ""),
        "screamingfrog_sitemap_source": data.get("screamingfrog_sitemap_source") or "",
        "screamingfrog_requested_homepage_url": data.get("screamingfrog_requested_homepage_url") or "",
        "screamingfrog_discovered_final_homepage_url": data.get("screamingfrog_discovered_final_homepage_url") or "",
        "screamingfrog_checked_at": data.get("screamingfrog_checked_at") or "",
        "screamingfrog_status": data.get("screamingfrog_status") or "",
        "screamingfrog_error_message": data.get("screamingfrog_error_message") or "",
        "screamingfrog_pages_crawled": data.get("screamingfrog_pages_crawled") or 0,
        "screamingfrog_homepage_status": data.get("screamingfrog_homepage_status") or "",
        "screamingfrog_homepage_status_code": data.get("screamingfrog_homepage_status_code") or "",
        "screamingfrog_title_issue_flags": split_pipe(data.get("screamingfrog_title_issue_flags")),
        "screamingfrog_meta_issue_flags": split_pipe(data.get("screamingfrog_meta_issue_flags")),
        "screamingfrog_canonical_issue_flags": split_pipe(data.get("screamingfrog_canonical_issue_flags")),
        "screamingfrog_internal_3xx_count": data.get("screamingfrog_internal_3xx_count") or 0,
        "screamingfrog_internal_4xx_count": data.get("screamingfrog_internal_4xx_count") or 0,
        "screamingfrog_internal_5xx_count": data.get("screamingfrog_internal_5xx_count") or 0,
        "screamingfrog_has_internal_errors": data.get("screamingfrog_has_internal_errors") or 0,
        "screamingfrog_location_page_count": data.get("screamingfrog_location_page_count") or 0,
        "screamingfrog_service_page_count": data.get("screamingfrog_service_page_count") or 0,
        "screamingfrog_category_page_count": data.get("screamingfrog_category_page_count") or 0,
        "screamingfrog_product_page_count": data.get("screamingfrog_product_page_count") or 0,
        "screamingfrog_schema_issue_flags": split_pipe(data.get("screamingfrog_schema_issue_flags")),
        "screamingfrog_collection_content_issue_flags": split_pipe(data.get("screamingfrog_collection_content_issue_flags")),
        "screamingfrog_product_metadata_issue_flags": split_pipe(data.get("screamingfrog_product_metadata_issue_flags")),
        "screamingfrog_default_title_issue_flags": split_pipe(data.get("screamingfrog_default_title_issue_flags")),
        "screamingfrog_homepage_issue_flags": split_pipe(data.get("screamingfrog_homepage_issue_flags")),
        "screamingfrog_heading_issue_flags": split_pipe(data.get("screamingfrog_heading_issue_flags")),
        "screamingfrog_heading_outline_score": data.get("screamingfrog_heading_outline_score") or 0,
        "screamingfrog_heading_outline_summary": data.get("screamingfrog_heading_outline_summary") or "",
        "screamingfrog_heading_pages_analyzed": data.get("screamingfrog_heading_pages_analyzed") or 0,
        "screamingfrog_heading_h1_missing_count": data.get("screamingfrog_heading_h1_missing_count") or 0,
        "screamingfrog_heading_multiple_h1_count": data.get("screamingfrog_heading_multiple_h1_count") or 0,
        "screamingfrog_heading_duplicate_h1_count": data.get("screamingfrog_heading_duplicate_h1_count") or 0,
        "screamingfrog_heading_pages_with_h2_count": data.get("screamingfrog_heading_pages_with_h2_count") or 0,
        "screamingfrog_heading_generic_heading_count": data.get("screamingfrog_heading_generic_heading_count") or 0,
        "screamingfrog_heading_repeated_heading_count": data.get("screamingfrog_heading_repeated_heading_count") or 0,
        "screamingfrog_opportunity_score": data.get("screamingfrog_opportunity_score") or 0,
        "screamingfrog_primary_issue_family": data.get("screamingfrog_primary_issue_family") or "",
        "screamingfrog_primary_issue_reason": data.get("screamingfrog_primary_issue_reason") or "",
        "screamingfrog_outreach_hooks": split_pipe_text(data.get("screamingfrog_outreach_hooks")),
        "screamingfrog_collection_detection_status": data.get("screamingfrog_collection_detection_status") or "",
        "screamingfrog_collection_detection_confidence": data.get("screamingfrog_collection_detection_confidence") or 0,
        "screamingfrog_collection_main_content": data.get("screamingfrog_collection_main_content") or "",
        "screamingfrog_collection_main_content_method": data.get("screamingfrog_collection_main_content_method") or "",
        "screamingfrog_collection_main_content_confidence": data.get("screamingfrog_collection_main_content_confidence") or 0,
        "screamingfrog_collection_above_raw_text": data.get("screamingfrog_collection_above_raw_text") or "",
        "screamingfrog_collection_below_raw_text": data.get("screamingfrog_collection_below_raw_text") or "",
        "screamingfrog_collection_above_clean_text": data.get("screamingfrog_collection_above_clean_text") or "",
        "screamingfrog_collection_below_clean_text": data.get("screamingfrog_collection_below_clean_text") or "",
        "screamingfrog_collection_best_intro_text": data.get("screamingfrog_collection_best_intro_text") or "",
        "screamingfrog_collection_best_intro_position": data.get("screamingfrog_collection_best_intro_position") or "",
        "screamingfrog_collection_best_intro_confidence": data.get("screamingfrog_collection_best_intro_confidence") or 0,
        "screamingfrog_collection_best_intro_source_type": data.get("screamingfrog_collection_best_intro_source_type") or "",
        "screamingfrog_collection_intro_text": data.get("screamingfrog_collection_intro_text") or "",
        "screamingfrog_collection_intro_position": data.get("screamingfrog_collection_intro_position") or "",
        "screamingfrog_collection_intro_status": data.get("screamingfrog_collection_intro_status") or "",
        "screamingfrog_collection_intro_method": data.get("screamingfrog_collection_intro_method") or "",
        "screamingfrog_collection_intro_confidence": data.get("screamingfrog_collection_intro_confidence") or 0,
        "screamingfrog_collection_schema_types": split_pipe_text(data.get("screamingfrog_collection_schema_types")),
        "screamingfrog_collection_schema_types_method": data.get("screamingfrog_collection_schema_types_method") or "",
        "screamingfrog_collection_schema_types_confidence": data.get("screamingfrog_collection_schema_types_confidence") or 0,
        "screamingfrog_collection_product_count": data.get("screamingfrog_collection_product_count") or 0,
        "screamingfrog_collection_product_count_method": data.get("screamingfrog_collection_product_count_method") or "",
        "screamingfrog_collection_product_count_confidence": data.get("screamingfrog_collection_product_count_confidence") or 0,
        "screamingfrog_collection_title_value": data.get("screamingfrog_collection_title_value") or "",
        "screamingfrog_collection_title_method": data.get("screamingfrog_collection_title_method") or "",
        "screamingfrog_collection_title_confidence": data.get("screamingfrog_collection_title_confidence") or 0,
        "screamingfrog_collection_h1_value": data.get("screamingfrog_collection_h1_value") or "",
        "screamingfrog_collection_h1_method": data.get("screamingfrog_collection_h1_method") or "",
        "screamingfrog_collection_h1_confidence": data.get("screamingfrog_collection_h1_confidence") or 0,
        "screamingfrog_title_optimization_status": data.get("screamingfrog_title_optimization_status") or "",
        "screamingfrog_title_optimization_confidence": data.get("screamingfrog_title_optimization_confidence") or 0,
        "screamingfrog_collection_title_rule_family": data.get("screamingfrog_collection_title_rule_family") or "",
        "screamingfrog_collection_title_rule_match": data.get("screamingfrog_collection_title_rule_match") or "",
        "screamingfrog_collection_title_rule_confidence": data.get("screamingfrog_collection_title_rule_confidence") or 0,
        "screamingfrog_collection_title_site_name_match": data.get("screamingfrog_collection_title_site_name_match") or 0,
        "screamingfrog_collection_issue_family": data.get("screamingfrog_collection_issue_family") or "",
        "screamingfrog_collection_issue_reason": data.get("screamingfrog_collection_issue_reason") or "",
        "screamingfrog_export_directory": data.get("screamingfrog_export_directory") or "",
        "bucket_reasons": " || ".join(bucket_reasons_raw),
        "bucket_reasons_list": bucket_reasons_raw,
        "bucket_count": len(sales_buckets),
        "contact_status": {
            "hasEmail": bool(data.get("emails")),
            "hasPhone": bool(data.get("telephones")),
            "hasPeople": bool(data.get("people")),
        },
    }


def build_seranking_join_and_select_sql() -> tuple[str, str]:
    join_sql = """
        left join (
            select snapshot.*
            from state.seranking_analysis_snapshots snapshot
            where not exists (
                select 1
                from state.seranking_analysis_snapshots newer
                where newer.root_domain = snapshot.root_domain
                  and newer.captured_at > snapshot.captured_at
            )
        ) se_ranking on se_ranking.root_domain = leads.root_domain
    """
    select_sql = """
        coalesce(se_ranking.analysis_type, '') as se_ranking_analysis_type,
        coalesce(se_ranking.analysis_mode, '') as se_ranking_analysis_mode,
        coalesce(se_ranking.date_mode, '') as se_ranking_date_mode,
        coalesce(se_ranking.regional_source, '') as se_ranking_market,
        coalesce(se_ranking.migration_likely_date, '') as se_ranking_migration_date,
        coalesce(se_ranking.baseline_month, '') as se_ranking_baseline_month,
        coalesce(se_ranking.comparison_month, '') as se_ranking_comparison_month,
        coalesce(se_ranking.first_comparison_month, '') as se_ranking_first_month,
        coalesce(se_ranking.second_comparison_month, '') as se_ranking_second_month,
        coalesce(se_ranking.date_label_first, '') as se_ranking_date_label_first,
        coalesce(se_ranking.date_label_second, '') as se_ranking_date_label_second,
        coalesce(se_ranking.traffic_before, '') as se_ranking_traffic_before,
        coalesce(se_ranking.traffic_last_month, '') as se_ranking_traffic_last_month,
        coalesce(se_ranking.traffic_delta_absolute, '') as se_ranking_traffic_delta_absolute,
        coalesce(se_ranking.traffic_delta_percent, '') as se_ranking_traffic_delta_percent,
        coalesce(se_ranking.keywords_before, '') as se_ranking_keywords_before,
        coalesce(se_ranking.keywords_last_month, '') as se_ranking_keywords_last_month,
        coalesce(se_ranking.keywords_delta_absolute, '') as se_ranking_keywords_delta_absolute,
        coalesce(se_ranking.keywords_delta_percent, '') as se_ranking_keywords_delta_percent,
        coalesce(se_ranking.price_before, '') as se_ranking_price_before,
        coalesce(se_ranking.price_last_month, '') as se_ranking_price_last_month,
        coalesce(se_ranking.price_delta_absolute, '') as se_ranking_price_delta_absolute,
        coalesce(se_ranking.price_delta_percent, '') as se_ranking_price_delta_percent,
        coalesce(se_ranking.outcome_flags, '') as se_ranking_outcome_flags,
        coalesce(se_ranking.captured_at, '') as se_ranking_checked_at,
        coalesce(se_ranking.status, '') as se_ranking_status,
        coalesce(se_ranking.error_message, '') as se_ranking_error_message
    """
    return join_sql, select_sql


def build_site_status_join_and_select_sql() -> tuple[str, str]:
    join_sql = """
        left join state.site_status_snapshots site_status
            on site_status.root_domain = leads.root_domain
    """
    select_sql = """
        coalesce(site_status.status_category, '') as site_status_category,
        coalesce(site_status.status_code, '') as site_status_code,
        coalesce(site_status.final_url, '') as site_status_final_url,
        coalesce(site_status.checked_at, '') as site_status_checked_at,
        coalesce(site_status.error_message, '') as site_status_error,
        coalesce(site_status.redirect_count, 0) as site_status_redirect_count
    """
    return join_sql, select_sql


def build_screamingfrog_join_and_select_sql() -> tuple[str, str]:
    join_sql = """
        left join state.screamingfrog_audit_snapshots screamingfrog
            on screamingfrog.root_domain = leads.root_domain
    """
    select_sql = """
        coalesce(screamingfrog.crawl_mode, '') as screamingfrog_crawl_mode,
        coalesce(screamingfrog.resolved_platform_family, '') as screamingfrog_resolved_platform_family,
        coalesce(screamingfrog.resolved_config_path, '') as screamingfrog_resolved_config_path,
        coalesce(screamingfrog.result_quality, '') as screamingfrog_result_quality,
        coalesce(screamingfrog.result_reason, '') as screamingfrog_result_reason,
        coalesce(screamingfrog.seed_strategy, '') as screamingfrog_seed_strategy,
        coalesce(screamingfrog.seed_count, 0) as screamingfrog_seed_count,
        coalesce(screamingfrog.sitemap_found, 0) as screamingfrog_sitemap_found,
        coalesce(screamingfrog.sitemap_url, '') as screamingfrog_sitemap_url,
        coalesce(screamingfrog.sitemap_source, '') as screamingfrog_sitemap_source,
        coalesce(screamingfrog.requested_homepage_url, '') as screamingfrog_requested_homepage_url,
        coalesce(screamingfrog.discovered_final_homepage_url, '') as screamingfrog_discovered_final_homepage_url,
        coalesce(screamingfrog.checked_at, '') as screamingfrog_checked_at,
        coalesce(screamingfrog.status, '') as screamingfrog_status,
        coalesce(screamingfrog.error_message, '') as screamingfrog_error_message,
        coalesce(screamingfrog.pages_crawled, 0) as screamingfrog_pages_crawled,
        coalesce(screamingfrog.homepage_status_category, '') as screamingfrog_homepage_status,
        coalesce(screamingfrog.homepage_status_code, '') as screamingfrog_homepage_status_code,
        coalesce(screamingfrog.title_issue_flags, '') as screamingfrog_title_issue_flags,
        coalesce(screamingfrog.meta_issue_flags, '') as screamingfrog_meta_issue_flags,
        coalesce(screamingfrog.canonical_issue_flags, '') as screamingfrog_canonical_issue_flags,
        coalesce(screamingfrog.internal_3xx_count, 0) as screamingfrog_internal_3xx_count,
        coalesce(screamingfrog.internal_4xx_count, 0) as screamingfrog_internal_4xx_count,
        coalesce(screamingfrog.internal_5xx_count, 0) as screamingfrog_internal_5xx_count,
        coalesce(screamingfrog.has_internal_errors, 0) as screamingfrog_has_internal_errors,
        coalesce(screamingfrog.location_page_count, 0) as screamingfrog_location_page_count,
        coalesce(screamingfrog.service_page_count, 0) as screamingfrog_service_page_count,
        coalesce(screamingfrog.category_page_count, 0) as screamingfrog_category_page_count,
        coalesce(screamingfrog.product_page_count, 0) as screamingfrog_product_page_count,
        coalesce(screamingfrog.schema_issue_flags, '') as screamingfrog_schema_issue_flags,
        coalesce(screamingfrog.collection_content_issue_flags, '') as screamingfrog_collection_content_issue_flags,
        coalesce(screamingfrog.product_metadata_issue_flags, '') as screamingfrog_product_metadata_issue_flags,
        coalesce(screamingfrog.default_title_issue_flags, '') as screamingfrog_default_title_issue_flags,
        coalesce(screamingfrog.homepage_issue_flags, '') as screamingfrog_homepage_issue_flags,
        coalesce(screamingfrog.heading_issue_flags, '') as screamingfrog_heading_issue_flags,
        coalesce(screamingfrog.heading_outline_score, 0) as screamingfrog_heading_outline_score,
        coalesce(screamingfrog.heading_outline_summary, '') as screamingfrog_heading_outline_summary,
        coalesce(screamingfrog.heading_pages_analyzed, 0) as screamingfrog_heading_pages_analyzed,
        coalesce(screamingfrog.heading_h1_missing_count, 0) as screamingfrog_heading_h1_missing_count,
        coalesce(screamingfrog.heading_multiple_h1_count, 0) as screamingfrog_heading_multiple_h1_count,
        coalesce(screamingfrog.heading_duplicate_h1_count, 0) as screamingfrog_heading_duplicate_h1_count,
        coalesce(screamingfrog.heading_pages_with_h2_count, 0) as screamingfrog_heading_pages_with_h2_count,
        coalesce(screamingfrog.heading_generic_heading_count, 0) as screamingfrog_heading_generic_heading_count,
        coalesce(screamingfrog.heading_repeated_heading_count, 0) as screamingfrog_heading_repeated_heading_count,
        coalesce(screamingfrog.sf_opportunity_score, 0) as screamingfrog_opportunity_score,
        coalesce(screamingfrog.sf_primary_issue_family, '') as screamingfrog_primary_issue_family,
        coalesce(screamingfrog.sf_primary_issue_reason, '') as screamingfrog_primary_issue_reason,
        coalesce(screamingfrog.sf_outreach_hooks, '') as screamingfrog_outreach_hooks,
        coalesce(screamingfrog.collection_detection_status, '') as screamingfrog_collection_detection_status,
        coalesce(screamingfrog.collection_detection_confidence, 0) as screamingfrog_collection_detection_confidence,
        coalesce(screamingfrog.collection_main_content, '') as screamingfrog_collection_main_content,
        coalesce(screamingfrog.collection_main_content_method, '') as screamingfrog_collection_main_content_method,
        coalesce(screamingfrog.collection_main_content_confidence, 0) as screamingfrog_collection_main_content_confidence,
        coalesce(screamingfrog.collection_above_raw_text, '') as screamingfrog_collection_above_raw_text,
        coalesce(screamingfrog.collection_below_raw_text, '') as screamingfrog_collection_below_raw_text,
        coalesce(screamingfrog.collection_above_clean_text, '') as screamingfrog_collection_above_clean_text,
        coalesce(screamingfrog.collection_below_clean_text, '') as screamingfrog_collection_below_clean_text,
        coalesce(screamingfrog.collection_best_intro_text, '') as screamingfrog_collection_best_intro_text,
        coalesce(screamingfrog.collection_best_intro_position, '') as screamingfrog_collection_best_intro_position,
        coalesce(screamingfrog.collection_best_intro_confidence, 0) as screamingfrog_collection_best_intro_confidence,
        coalesce(screamingfrog.collection_best_intro_source_type, '') as screamingfrog_collection_best_intro_source_type,
        coalesce(screamingfrog.collection_intro_text, '') as screamingfrog_collection_intro_text,
        coalesce(screamingfrog.collection_intro_position, '') as screamingfrog_collection_intro_position,
        coalesce(screamingfrog.collection_intro_status, '') as screamingfrog_collection_intro_status,
        coalesce(screamingfrog.collection_intro_method, '') as screamingfrog_collection_intro_method,
        coalesce(screamingfrog.collection_intro_confidence, 0) as screamingfrog_collection_intro_confidence,
        coalesce(screamingfrog.collection_schema_types, '') as screamingfrog_collection_schema_types,
        coalesce(screamingfrog.collection_schema_types_method, '') as screamingfrog_collection_schema_types_method,
        coalesce(screamingfrog.collection_schema_types_confidence, 0) as screamingfrog_collection_schema_types_confidence,
        coalesce(screamingfrog.collection_product_count, 0) as screamingfrog_collection_product_count,
        coalesce(screamingfrog.collection_product_count_method, '') as screamingfrog_collection_product_count_method,
        coalesce(screamingfrog.collection_product_count_confidence, 0) as screamingfrog_collection_product_count_confidence,
        coalesce(screamingfrog.collection_title_value, '') as screamingfrog_collection_title_value,
        coalesce(screamingfrog.collection_title_method, '') as screamingfrog_collection_title_method,
        coalesce(screamingfrog.collection_title_confidence, 0) as screamingfrog_collection_title_confidence,
        coalesce(screamingfrog.collection_h1_value, '') as screamingfrog_collection_h1_value,
        coalesce(screamingfrog.collection_h1_method, '') as screamingfrog_collection_h1_method,
        coalesce(screamingfrog.collection_h1_confidence, 0) as screamingfrog_collection_h1_confidence,
        coalesce(screamingfrog.title_optimization_status, '') as screamingfrog_title_optimization_status,
        coalesce(screamingfrog.title_optimization_confidence, 0) as screamingfrog_title_optimization_confidence,
        coalesce(screamingfrog.collection_title_rule_family, '') as screamingfrog_collection_title_rule_family,
        coalesce(screamingfrog.collection_title_rule_match, '') as screamingfrog_collection_title_rule_match,
        coalesce(screamingfrog.collection_title_rule_confidence, 0) as screamingfrog_collection_title_rule_confidence,
        coalesce(screamingfrog.collection_title_site_name_match, 0) as screamingfrog_collection_title_site_name_match,
        coalesce(screamingfrog.collection_issue_family, '') as screamingfrog_collection_issue_family,
        coalesce(screamingfrog.collection_issue_reason, '') as screamingfrog_collection_issue_reason,
        coalesce(screamingfrog.export_directory, '') as screamingfrog_export_directory
    """
    return join_sql, select_sql


def get_selected_tray_domains(connection: sqlite3.Connection) -> list[str]:
    return [
        row["root_domain"]
        for row in connection.execute(
            "select root_domain from state.export_tray_items order by added_at desc"
        ).fetchall()
    ]


def selected_tray_analysis_candidates(
    connection: sqlite3.Connection,
    analysis_type: str,
) -> list[dict[str, Any]]:
    normalized_type = normalize_seranking_analysis_type(analysis_type)
    domains = get_selected_tray_domains(connection)
    if not domains:
        return []
    placeholders = ", ".join("?" for _ in domains)
    rows = connection.execute(
        f"""
        select leads.root_domain,
               leads.country,
               leads.company,
               leads.cms_migration_likely_date,
               leads.cms_migration_status,
               domain_migration.domain_migration_estimated_date,
               domain_migration.domain_migration_status
        from leads
        left join domain_migration_best_match_ui domain_migration
            on domain_migration.current_domain = leads.root_domain
        where leads.root_domain in ({placeholders})
        """,
        domains,
    ).fetchall()
    existing = {
        (row["root_domain"], row["analysis_type"])
        for row in connection.execute(
            "select root_domain, analysis_type from state.seranking_analysis_snapshots"
        ).fetchall()
    }
    candidates: list[dict[str, Any]] = []
    for row in rows:
        migration_date = (
            row["cms_migration_likely_date"]
            if normalized_type == "cms_migration"
            else row["domain_migration_estimated_date"]
        )
        migration_status = (
            row["cms_migration_status"]
            if normalized_type == "cms_migration"
            else row["domain_migration_status"]
        )
        migration_date_value = parse_iso_date(migration_date)
        market = SERANKING_SOURCE_MAP.get(row["country"] or "")
        eligibility_reason = ""
        eligible = True
        if not migration_date_value:
            eligible = False
            eligibility_reason = "No likely migration date"
        elif not market:
            eligible = False
            eligibility_reason = "No SE Ranking market mapping"
        elif normalized_type == "cms_migration" and migration_status in {"none", "removed_only"}:
            eligible = False
            eligibility_reason = "No qualifying CMS migration"
        elif normalized_type == "domain_migration" and migration_status == "none":
            eligible = False
            eligibility_reason = "No qualifying domain migration"
        candidates.append(
            {
                "root_domain": row["root_domain"],
                "company": row["company"],
                "country": row["country"],
                "analysis_type": normalized_type,
                "migration_likely_date": migration_date or "",
                "regional_source": market or "",
                "baseline_month": month_key(shift_months(month_start_for_date(migration_date_value), -1))
                if migration_date_value
                else "",
                "comparison_month": month_key(month_start_for_date(datetime.now(UTC).date())),
                "eligible": eligible,
                "eligibility_reason": eligibility_reason,
                "already_analyzed": (row["root_domain"], normalized_type) in existing,
            }
        )
    candidates.sort(key=lambda item: item["root_domain"])
    return candidates


def filtered_view_analysis_candidates(
    connection: sqlite3.Connection,
    analysis_type: str,
    *,
    filters: Mapping[str, Any],
) -> list[dict[str, Any]]:
    normalized_type = normalize_seranking_analysis_type(analysis_type)
    _where, _params, rows = fetch_filtered_rows(connection, filters=filters)
    existing = {
        (row["root_domain"], row["analysis_type"])
        for row in connection.execute(
            "select root_domain, analysis_type from state.seranking_analysis_snapshots"
        ).fetchall()
    }
    candidates: list[dict[str, Any]] = []
    for row in rows:
        migration_date = (
            row["cms_migration_likely_date"]
            if normalized_type == "cms_migration"
            else row["domain_migration_estimated_date"]
        )
        migration_status = (
            row["cms_migration_status"]
            if normalized_type == "cms_migration"
            else row["domain_migration_status"]
        )
        migration_date_value = parse_iso_date(migration_date)
        market = SERANKING_SOURCE_MAP.get(row["country"] or "")
        eligibility_reason = ""
        eligible = True
        if not migration_date_value:
            eligible = False
            eligibility_reason = "No likely migration date"
        elif not market:
            eligible = False
            eligibility_reason = "No SE Ranking market mapping"
        elif normalized_type == "cms_migration" and migration_status in {"none", "removed_only"}:
            eligible = False
            eligibility_reason = "No qualifying CMS migration"
        elif normalized_type == "domain_migration" and migration_status == "none":
            eligible = False
            eligibility_reason = "No qualifying domain migration"
        candidates.append(
            {
                "root_domain": row["root_domain"],
                "company": row["company"],
                "country": row["country"],
                "analysis_type": normalized_type,
                "migration_likely_date": migration_date or "",
                "regional_source": market or "",
                "baseline_month": month_key(shift_months(month_start_for_date(migration_date_value), -1))
                if migration_date_value
                else "",
                "comparison_month": month_key(month_start_for_date(datetime.now(UTC).date())),
                "eligible": eligible,
                "eligibility_reason": eligibility_reason,
                "already_analyzed": (row["root_domain"], normalized_type) in existing,
            }
        )
    candidates.sort(key=lambda item: item["root_domain"])
    return candidates


def resolve_manual_analysis_domains(
    connection: sqlite3.Connection,
    *,
    root_domains: list[str],
    use_selected_tray: bool,
) -> list[str]:
    if root_domains:
        return sorted({normalize_domain_search(domain) for domain in root_domains if normalize_domain_search(domain)})
    if use_selected_tray:
        return get_selected_tray_domains(connection)
    return []


def manual_analysis_candidates(
    connection: sqlite3.Connection,
    *,
    root_domains: list[str],
    use_selected_tray: bool,
    first_month: str,
    second_month: str,
) -> list[dict[str, Any]]:
    domains = resolve_manual_analysis_domains(connection, root_domains=root_domains, use_selected_tray=use_selected_tray)
    if not domains:
        return []
    placeholders = ", ".join("?" for _ in domains)
    rows = connection.execute(
        f"""
        select leads.root_domain, leads.country, leads.company
        from leads
        where leads.root_domain in ({placeholders})
        order by leads.root_domain asc
        """,
        domains,
    ).fetchall()
    existing = {
        row["root_domain"]
        for row in connection.execute(
            "select root_domain from state.seranking_analysis_snapshots where analysis_type = 'manual_comparison'"
        ).fetchall()
    }
    candidates: list[dict[str, Any]] = []
    for row in rows:
        market = SERANKING_SOURCE_MAP.get(row["country"] or "")
        eligible = bool(market)
        candidates.append(
            {
                "root_domain": row["root_domain"],
                "company": row["company"],
                "country": row["country"],
                "analysis_type": "manual_comparison",
                "analysis_mode": "manual",
                "date_mode": "manual",
                "migration_likely_date": "",
                "regional_source": market or "",
                "baseline_month": first_month,
                "comparison_month": second_month,
                "first_comparison_month": first_month,
                "second_comparison_month": second_month,
                "date_label_first": first_month,
                "date_label_second": second_month,
                "eligible": eligible,
                "eligibility_reason": "" if eligible else "No SE Ranking market mapping",
                "already_analyzed": row["root_domain"] in existing,
            }
        )
    return candidates


def summarize_seranking_candidates(candidates: list[dict[str, Any]], *, skip_existing: bool) -> dict[str, Any]:
    eligible = [item for item in candidates if item["eligible"]]
    already_analyzed = [item for item in eligible if item["already_analyzed"]]
    to_run = [item for item in eligible if not item["already_analyzed"] or not skip_existing]
    return {
        "selectedCount": len(candidates),
        "eligibleCount": len(eligible),
        "alreadyAnalyzedCount": len(already_analyzed),
        "toRunCount": len(to_run),
        "estimatedRequests": len(to_run),
        "estimatedCredits": len(to_run) * 100,
        "excluded": [
            {
                "root_domain": item["root_domain"],
                "reason": item["eligibility_reason"],
            }
            for item in candidates
            if not item["eligible"]
        ],
    }


def validate_manual_months(first_month: str, second_month: str) -> tuple[str, str]:
    normalized_first = parse_month_key(first_month)
    normalized_second = parse_month_key(second_month)
    if not normalized_first or not normalized_second:
        raise HTTPException(status_code=400, detail="Months must be in YYYY-MM format")
    if normalized_first == normalized_second:
        raise HTTPException(status_code=400, detail="Choose two different comparison months")
    return normalized_first, normalized_second


def extract_monthly_history_rows(history_rows: list[dict[str, Any]], baseline_month: str, comparison_month: str) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    by_month = {
        f"{int(row.get('year', 0)):04d}-{int(row.get('month', 0)):02d}": row
        for row in history_rows
        if row.get("year") and row.get("month")
    }
    return by_month.get(baseline_month), by_month.get(comparison_month)


def persist_seranking_snapshot(connection: sqlite3.Connection, payload: dict[str, Any]) -> None:
    connection.execute(
        """
        insert into state.seranking_analysis_snapshots (
            root_domain, analysis_type, analysis_mode, date_mode, regional_source, migration_likely_date, baseline_month, comparison_month,
            first_comparison_month, second_comparison_month, date_label_first, date_label_second,
            traffic_before, traffic_last_month, traffic_delta_absolute, traffic_delta_percent,
            keywords_before, keywords_last_month, keywords_delta_absolute, keywords_delta_percent,
            price_before, price_last_month, price_delta_absolute, price_delta_percent,
            outcome_flags, captured_at, status, error_message
        ) values (
            :root_domain, :analysis_type, :analysis_mode, :date_mode, :regional_source, :migration_likely_date, :baseline_month, :comparison_month,
            :first_comparison_month, :second_comparison_month, :date_label_first, :date_label_second,
            :traffic_before, :traffic_last_month, :traffic_delta_absolute, :traffic_delta_percent,
            :keywords_before, :keywords_last_month, :keywords_delta_absolute, :keywords_delta_percent,
            :price_before, :price_last_month, :price_delta_absolute, :price_delta_percent,
            :outcome_flags, :captured_at, :status, :error_message
        )
        on conflict(root_domain, analysis_type) do update set
            analysis_mode=excluded.analysis_mode,
            date_mode=excluded.date_mode,
            regional_source=excluded.regional_source,
            migration_likely_date=excluded.migration_likely_date,
            baseline_month=excluded.baseline_month,
            comparison_month=excluded.comparison_month,
            first_comparison_month=excluded.first_comparison_month,
            second_comparison_month=excluded.second_comparison_month,
            date_label_first=excluded.date_label_first,
            date_label_second=excluded.date_label_second,
            traffic_before=excluded.traffic_before,
            traffic_last_month=excluded.traffic_last_month,
            traffic_delta_absolute=excluded.traffic_delta_absolute,
            traffic_delta_percent=excluded.traffic_delta_percent,
            keywords_before=excluded.keywords_before,
            keywords_last_month=excluded.keywords_last_month,
            keywords_delta_absolute=excluded.keywords_delta_absolute,
            keywords_delta_percent=excluded.keywords_delta_percent,
            price_before=excluded.price_before,
            price_last_month=excluded.price_last_month,
            price_delta_absolute=excluded.price_delta_absolute,
            price_delta_percent=excluded.price_delta_percent,
            outcome_flags=excluded.outcome_flags,
            captured_at=excluded.captured_at,
            status=excluded.status,
            error_message=excluded.error_message
        """,
        payload,
    )


def run_seranking_analysis(
    connection: sqlite3.Connection,
    analysis_type: str,
    *,
    refresh_existing: bool,
    filters: Mapping[str, Any] | None = None,
    use_filtered_view: bool = False,
) -> dict[str, Any]:
    normalized_type = normalize_seranking_analysis_type(analysis_type)
    candidates = (
        filtered_view_analysis_candidates(connection, normalized_type, filters=filters or {})
        if use_filtered_view
        else selected_tray_analysis_candidates(connection, normalized_type)
    )
    summary = summarize_seranking_candidates(candidates, skip_existing=not refresh_existing)
    to_run = [
        item
        for item in candidates
        if item["eligible"] and (refresh_existing or not item["already_analyzed"])
    ]
    if not to_run:
        return {"summary": summary, "results": []}

    results: list[dict[str, Any]] = []
    for item in to_run:
        captured_at = now_iso()
        payload: dict[str, Any] = {
            **item,
            "analysis_mode": "migration",
            "date_mode": "migration",
            "first_comparison_month": item["baseline_month"],
            "second_comparison_month": item["comparison_month"],
            "date_label_first": item["baseline_month"],
            "date_label_second": "Today",
            "traffic_before": None,
            "traffic_last_month": None,
            "traffic_delta_absolute": None,
            "traffic_delta_percent": None,
            "keywords_before": None,
            "keywords_last_month": None,
            "keywords_delta_absolute": None,
            "keywords_delta_percent": None,
            "price_before": None,
            "price_last_month": None,
            "price_delta_absolute": None,
            "price_delta_percent": None,
            "outcome_flags": "",
            "captured_at": captured_at,
            "status": "error",
            "error_message": "",
        }
        try:
            history_rows = seranking_history(item["root_domain"], item["regional_source"])
            baseline_row, comparison_row = extract_monthly_history_rows(
                history_rows,
                item["baseline_month"],
                item["comparison_month"],
            )
            if not baseline_row or not comparison_row:
                payload["status"] = "partial"
                payload["error_message"] = "Missing baseline or comparison month in SE Ranking history"
            else:
                traffic_before = int(baseline_row.get("traffic_sum") or 0)
                traffic_last_month = int(comparison_row.get("traffic_sum") or 0)
                keywords_before = int(baseline_row.get("keywords_count") or 0)
                keywords_last_month = int(comparison_row.get("keywords_count") or 0)
                price_before = float(baseline_row.get("price_sum") or 0)
                price_last_month = float(comparison_row.get("price_sum") or 0)
                traffic_delta_percent = safe_percent_delta(traffic_before, traffic_last_month)
                keywords_delta_percent = safe_percent_delta(keywords_before, keywords_last_month)
                price_delta_percent = safe_percent_delta(price_before, price_last_month)
                outcome_flags = build_seranking_outcome_flags(traffic_delta_percent, keywords_delta_percent)
                payload.update(
                    {
                        "traffic_before": traffic_before,
                        "traffic_last_month": traffic_last_month,
                        "traffic_delta_absolute": traffic_last_month - traffic_before,
                        "traffic_delta_percent": traffic_delta_percent,
                        "keywords_before": keywords_before,
                        "keywords_last_month": keywords_last_month,
                        "keywords_delta_absolute": keywords_last_month - keywords_before,
                        "keywords_delta_percent": keywords_delta_percent,
                        "price_before": price_before,
                        "price_last_month": price_last_month,
                        "price_delta_absolute": price_last_month - price_before,
                        "price_delta_percent": price_delta_percent,
                        "outcome_flags": "|".join(outcome_flags),
                        "status": "success",
                    }
                )
        except HTTPException as exc:
            payload["status"] = "error"
            payload["error_message"] = str(exc.detail)

        persist_seranking_snapshot(connection, payload)
        results.append(
            {
                "root_domain": item["root_domain"],
                "status": payload["status"],
                "error_message": payload["error_message"],
            }
        )

    connection.commit()
    return {"summary": summary, "results": results}


def run_manual_seranking_analysis(
    connection: sqlite3.Connection,
    *,
    root_domains: list[str],
    use_selected_tray: bool,
    first_month: str,
    second_month: str,
) -> dict[str, Any]:
    normalized_first, normalized_second = validate_manual_months(first_month, second_month)
    candidates = manual_analysis_candidates(
        connection,
        root_domains=root_domains,
        use_selected_tray=use_selected_tray,
        first_month=normalized_first,
        second_month=normalized_second,
    )
    summary = summarize_seranking_candidates(candidates, skip_existing=False)
    to_run = [item for item in candidates if item["eligible"]]
    if not to_run:
        return {"analysisType": "manual_comparison", "analysisMode": "manual", "summary": summary, "results": []}

    results: list[dict[str, Any]] = []
    for item in to_run:
        captured_at = now_iso()
        payload: dict[str, Any] = {
            **item,
            "traffic_before": None,
            "traffic_last_month": None,
            "traffic_delta_absolute": None,
            "traffic_delta_percent": None,
            "keywords_before": None,
            "keywords_last_month": None,
            "keywords_delta_absolute": None,
            "keywords_delta_percent": None,
            "price_before": None,
            "price_last_month": None,
            "price_delta_absolute": None,
            "price_delta_percent": None,
            "outcome_flags": "",
            "captured_at": captured_at,
            "status": "error",
            "error_message": "",
        }
        try:
            history_rows = seranking_history(item["root_domain"], item["regional_source"])
            first_row, second_row = extract_monthly_history_rows(
                history_rows,
                item["first_comparison_month"],
                item["second_comparison_month"],
            )
            if not first_row or not second_row:
                payload["status"] = "partial"
                payload["error_message"] = "Missing first or second comparison month in SE Ranking history"
            else:
                traffic_before = int(first_row.get("traffic_sum") or 0)
                traffic_last_month = int(second_row.get("traffic_sum") or 0)
                keywords_before = int(first_row.get("keywords_count") or 0)
                keywords_last_month = int(second_row.get("keywords_count") or 0)
                price_before = float(first_row.get("price_sum") or 0)
                price_last_month = float(second_row.get("price_sum") or 0)
                traffic_delta_percent = safe_percent_delta(traffic_before, traffic_last_month)
                keywords_delta_percent = safe_percent_delta(keywords_before, keywords_last_month)
                price_delta_percent = safe_percent_delta(price_before, price_last_month)
                outcome_flags = build_seranking_outcome_flags(traffic_delta_percent, keywords_delta_percent)
                payload.update(
                    {
                        "traffic_before": traffic_before,
                        "traffic_last_month": traffic_last_month,
                        "traffic_delta_absolute": traffic_last_month - traffic_before,
                        "traffic_delta_percent": traffic_delta_percent,
                        "keywords_before": keywords_before,
                        "keywords_last_month": keywords_last_month,
                        "keywords_delta_absolute": keywords_last_month - keywords_before,
                        "keywords_delta_percent": keywords_delta_percent,
                        "price_before": price_before,
                        "price_last_month": price_last_month,
                        "price_delta_absolute": price_last_month - price_before,
                        "price_delta_percent": price_delta_percent,
                        "outcome_flags": "|".join(outcome_flags),
                        "status": "success",
                    }
                )
        except HTTPException as exc:
            payload["status"] = "error"
            payload["error_message"] = str(exc.detail)

        persist_seranking_snapshot(connection, payload)
        results.append(
            {
                "root_domain": item["root_domain"],
                "status": payload["status"],
                "error_message": payload["error_message"],
            }
        )

    connection.commit()
    return {"analysisType": "manual_comparison", "analysisMode": "manual", "summary": summary, "results": results}


def selected_tray_site_status_candidates(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    domains = get_selected_tray_domains(connection)
    if not domains:
        return []
    placeholders = ", ".join("?" for _ in domains)
    rows = connection.execute(
        f"""
        select leads.root_domain, leads.company, coalesce(leads.current_platforms, '') as current_platforms
        from leads
        where leads.root_domain in ({placeholders})
        order by leads.root_domain asc
        """,
        domains,
    ).fetchall()
    existing = {
        row["root_domain"]
        for row in connection.execute("select root_domain from state.site_status_snapshots").fetchall()
    }
    return [
        {
            "root_domain": row["root_domain"],
            "company": row["company"],
            "eligible": True,
            "eligibility_reason": "",
            "already_checked": row["root_domain"] in existing,
        }
        for row in rows
    ]


def summarize_site_status_candidates(candidates: list[dict[str, Any]], *, skip_existing: bool) -> dict[str, Any]:
    selected_count = len(candidates)
    eligible = [item for item in candidates if item["eligible"]]
    already_checked = [item for item in eligible if item["already_checked"]]
    to_run = [item for item in eligible if not skip_existing or not item["already_checked"]]
    excluded = [{"root_domain": item["root_domain"], "reason": item["eligibility_reason"]} for item in candidates if not item["eligible"]]
    return {
        "selectedCount": selected_count,
        "eligibleCount": len(eligible),
        "alreadyCheckedCount": len(already_checked),
        "toRunCount": len(to_run),
        "estimatedRequests": len(to_run),
        "excluded": excluded,
    }


def classify_site_status(status_code: int | None, final_url: str, requested_url: str, error_message: str) -> str:
    if status_code in {403, 429}:
        return "blocked"
    if status_code == 404:
        return "not_found"
    if status_code is not None and 500 <= status_code <= 599:
        return "server_error"
    if status_code is not None and 200 <= status_code <= 299:
        return "redirect" if final_url and final_url.rstrip("/") != requested_url.rstrip("/") else "ok"
    if "timed out" in error_message.lower():
        return "timeout"
    if "certificate" in error_message.lower() or "ssl" in error_message.lower():
        return "ssl_error"
    if "name or service not known" in error_message.lower() or "nodename nor servname provided" in error_message.lower():
        return "dns_error"
    return "other_error"


def perform_site_status_check(root_domain: str) -> dict[str, Any]:
    headers = {"User-Agent": "Mozilla/5.0 (compatible; DOMAIN DEALER/1.0; +https://domain-dealer.local)"}
    attempts = [f"https://{root_domain}", f"http://{root_domain}"]
    errors: list[str] = []
    for index, requested_url in enumerate(attempts):
        request = Request(requested_url, headers=headers, method="GET")
        try:
            with urlopen(request, timeout=10) as response:
                final_url = response.geturl()
                status_code = int(getattr(response, "status", response.getcode()) or 0)
                redirect_count = max(len(getattr(response, "url", final_url).split("://", 1)) - 1, 0)
                return {
                    "requested_url": requested_url,
                    "final_url": final_url,
                    "status_code": status_code,
                    "status_category": classify_site_status(status_code, final_url, requested_url, ""),
                    "redirect_count": redirect_count if final_url != requested_url else 0,
                    "error_message": "",
                }
        except HTTPError as exc:
            final_url = exc.geturl() or requested_url
            status_code = int(exc.code)
            return {
                "requested_url": requested_url,
                "final_url": final_url,
                "status_code": status_code,
                "status_category": classify_site_status(status_code, final_url, requested_url, ""),
                "redirect_count": 0 if final_url == requested_url else 1,
                "error_message": "",
            }
        except URLError as exc:
            reason = exc.reason
            message = str(reason)
            if isinstance(reason, socket.timeout):
                message = "Request timed out"
            elif isinstance(reason, ssl.SSLError):
                message = f"SSL error: {reason}"
            errors.append(message)
            if index == 0:
                continue
        except Exception as exc:  # noqa: BLE001
            errors.append(str(exc))
            if index == 0:
                continue
    error_message = " | ".join(error for error in errors if error) or "Unknown request error"
    return {
        "requested_url": attempts[0],
        "final_url": "",
        "status_code": None,
        "status_category": classify_site_status(None, "", attempts[0], error_message),
        "redirect_count": 0,
        "error_message": error_message,
    }


def persist_site_status_snapshot(connection: sqlite3.Connection, payload: dict[str, Any]) -> None:
    connection.execute(
        """
        insert into state.site_status_snapshots (
            root_domain,
            requested_url,
            final_url,
            status_code,
            status_category,
            redirect_count,
            checked_at,
            error_message
        ) values (
            :root_domain,
            :requested_url,
            :final_url,
            :status_code,
            :status_category,
            :redirect_count,
            :checked_at,
            :error_message
        )
        on conflict(root_domain) do update set
            requested_url=excluded.requested_url,
            final_url=excluded.final_url,
            status_code=excluded.status_code,
            status_category=excluded.status_category,
            redirect_count=excluded.redirect_count,
            checked_at=excluded.checked_at,
            error_message=excluded.error_message
        """,
        payload,
    )


def run_site_status_checks(connection: sqlite3.Connection, *, refresh_existing: bool) -> dict[str, Any]:
    candidates = selected_tray_site_status_candidates(connection)
    summary = summarize_site_status_candidates(candidates, skip_existing=not refresh_existing)
    to_run = [item for item in candidates if item["eligible"] and (refresh_existing or not item["already_checked"])]
    if not to_run:
        return {"summary": summary, "results": []}

    results: list[dict[str, Any]] = []
    for item in to_run:
        checked_at = now_iso()
        outcome = perform_site_status_check(item["root_domain"])
        persist_site_status_snapshot(
            connection,
            {
                "root_domain": item["root_domain"],
                "requested_url": outcome["requested_url"],
                "final_url": outcome["final_url"],
                "status_code": outcome["status_code"],
                "status_category": outcome["status_category"],
                "redirect_count": outcome["redirect_count"],
                "checked_at": checked_at,
                "error_message": outcome["error_message"],
            },
        )
        results.append(
            {
                "root_domain": item["root_domain"],
                "status": outcome["status_category"],
                "error_message": outcome["error_message"],
            }
        )

    connection.commit()
    return {"summary": summary, "results": results}


def normalize_screamingfrog_crawl_mode(value: str | None) -> str:
    return value if value in SCREAMINGFROG_CRAWL_MODES else "bounded_audit"


def normalize_screamingfrog_platform_family(value: str | None) -> str:
    return value if value in SCREAMINGFROG_PLATFORM_FAMILIES else "generic"


def screamingfrog_mode_folder(crawl_mode: str) -> str:
    return "bounded" if normalize_screamingfrog_crawl_mode(crawl_mode) == "bounded_audit" else "deep"


def resolve_screamingfrog_launcher() -> Path | None:
    candidates = [
        SCREAMINGFROG_LAUNCHER_ENV,
        "/Applications/Screaming Frog SEO Spider.app/Contents/MacOS/ScreamingFrogSEOSpiderLauncher",
    ]
    for candidate in candidates:
        if not candidate:
            continue
        path = Path(candidate).expanduser()
        if path.exists() and os.access(path, os.X_OK):
            return path
    return None


def resolve_screamingfrog_legacy_app_config(crawl_mode: str) -> Path | None:
    normalized_mode = normalize_screamingfrog_crawl_mode(crawl_mode)
    env_candidate = SCREAMINGFROG_BOUNDED_CONFIG_ENV if normalized_mode == "bounded_audit" else SCREAMINGFROG_DEEP_CONFIG_ENV
    candidates = [
        env_candidate,
        SCREAMINGFROG_CONFIG_ROOT / screamingfrog_mode_folder(normalized_mode) / f"{normalized_mode}.seospiderconfig",
    ]
    for candidate in candidates:
        if not candidate:
            continue
        path = Path(candidate).expanduser() if isinstance(candidate, str) else candidate
        if path.exists():
            return path
    return None


def resolve_screamingfrog_platform_family(current_platforms: list[str]) -> str:
    normalized = {platform.strip().lower() for platform in current_platforms if platform.strip()}
    for candidate, family in SCREAMINGFROG_PLATFORM_PRIORITY:
        if candidate in normalized:
            return family
    return "generic"


def resolve_screamingfrog_profile_path(crawl_mode: str, platform_family: str) -> Path:
    normalized_mode = normalize_screamingfrog_crawl_mode(crawl_mode)
    normalized_family = normalize_screamingfrog_platform_family(platform_family)
    candidate = SCREAMINGFROG_CONFIG_ROOT / screamingfrog_mode_folder(normalized_mode) / f"{normalized_family}.json"
    if candidate.exists():
        return candidate
    return SCREAMINGFROG_CONFIG_ROOT / screamingfrog_mode_folder(normalized_mode) / "generic.json"


def load_screamingfrog_profile(crawl_mode: str, platform_family: str) -> tuple[dict[str, Any], Path]:
    path = resolve_screamingfrog_profile_path(crawl_mode, platform_family)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        if platform_family != "generic":
            generic_path = SCREAMINGFROG_CONFIG_ROOT / screamingfrog_mode_folder(crawl_mode) / "generic.json"
            payload = json.loads(generic_path.read_text(encoding="utf-8"))
            return payload, generic_path
        raise
    return payload, path


def domain_requires_cautious_crawl(root_domain: str) -> bool:
    try:
        with get_state_connection() as connection:
            snapshot_row = connection.execute(
                """
                select result_reason, error_message
                from screamingfrog_audit_snapshots
                where root_domain = ?
                """,
                (root_domain,),
            ).fetchone()
            job_rows = connection.execute(
                """
                select result_reason, message
                from screamingfrog_jobs
                where root_domain = ?
                """,
                (root_domain,),
            ).fetchall()
    except sqlite3.Error:
        return False
    for row in [snapshot_row, *job_rows]:
        if not row:
            continue
        result_reason = str(row["result_reason"] or "").strip().lower()
        message = str((row["error_message"] if "error_message" in row.keys() else row["message"]) or "").strip().lower()
        if result_reason == "rate_limited_429" or "429 too many requests" in message:
            return True
    return False


def apply_cautious_screamingfrog_profile(profile: Mapping[str, Any], *, crawl_mode: str, root_domain: str) -> dict[str, Any]:
    adjusted = dict(profile)
    normalized_mode = normalize_screamingfrog_crawl_mode(crawl_mode)
    adjusted["timeout_seconds"] = max(parse_int_value(adjusted.get("timeout_seconds")), 240 if normalized_mode == "bounded_audit" else 420)
    if normalized_mode == "bounded_audit":
        adjusted["max_seed_urls"] = min(parse_int_value(adjusted.get("max_seed_urls")) or 10, 10)
        adjusted["max_category_urls"] = min(parse_int_value(adjusted.get("max_category_urls")) or 5, 5)
        adjusted["max_product_urls"] = min(parse_int_value(adjusted.get("max_product_urls")) or 4, 3)
        adjusted["max_service_urls"] = min(parse_int_value(adjusted.get("max_service_urls")) or 3, 2)
        adjusted["max_location_urls"] = min(parse_int_value(adjusted.get("max_location_urls")) or 2, 2)
        adjusted["max_other_urls"] = min(parse_int_value(adjusted.get("max_other_urls")) or 1, 1)
    if domain_requires_cautious_crawl(root_domain):
        adjusted["max_seed_urls"] = min(parse_int_value(adjusted.get("max_seed_urls")) or 6, 4)
        adjusted["max_category_urls"] = min(parse_int_value(adjusted.get("max_category_urls")) or 2, 1)
        adjusted["max_product_urls"] = min(parse_int_value(adjusted.get("max_product_urls")) or 3, 2)
        adjusted["max_service_urls"] = min(parse_int_value(adjusted.get("max_service_urls")) or 2, 1)
        adjusted["max_location_urls"] = min(parse_int_value(adjusted.get("max_location_urls")) or 2, 1)
        adjusted["max_other_urls"] = 0
        adjusted["timeout_seconds"] = max(parse_int_value(adjusted.get("timeout_seconds")), 300)
        adjusted["cautious_crawl"] = True
    return adjusted


def safe_slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9.-]+", "-", value.lower()).strip("-")
    return slug or "domain"


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return [dict(row) for row in reader]


def parse_int_value(value: Any) -> int:
    try:
        return int(float(str(value or "0").replace(",", "")))
    except (TypeError, ValueError):
        return 0


def parse_csv_address(value: str | None) -> str:
    if not value:
        return ""
    return value.strip()


def csv_row_value(row: Mapping[str, Any], key: str) -> str:
    candidates = (
        key,
        f'"{key}"',
        f"\ufeff{key}",
        f'\ufeff"{key}"',
    )
    for candidate in candidates:
        if candidate in row and row.get(candidate) is not None:
            return str(row.get(candidate) or "").strip()
    return ""


def split_pipe_text(value: str | None) -> list[str]:
    return [item.strip() for item in (value or "").split("|") if item.strip()]


def normalized_url_key(value: str | None) -> str:
    if not value:
        return ""
    normalized = value.strip().lower()
    normalized = normalized.removeprefix("http://").removeprefix("https://")
    normalized = normalized.rstrip("/")
    return normalized


def normalize_seed_url(value: str) -> str:
    parsed = urlparse(value)
    cleaned = parsed._replace(params="", query="", fragment="")
    path = cleaned.path or "/"
    cleaned = cleaned._replace(path=path)
    return urlunparse(cleaned).rstrip("/") or urlunparse(cleaned)


def detect_screamingfrog_rate_limited_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    flagged: list[dict[str, str]] = []
    for row in rows:
        status_code = parse_int_value(csv_row_value(row, "Status Code"))
        status_text = csv_row_value(row, "Status").lower()
        if status_code == 429 or "too many requests" in status_text:
            flagged.append(row)
    return flagged


def url_matches_same_site(candidate: str, homepage_url: str, root_domain: str) -> bool:
    parsed_candidate = urlparse(candidate)
    parsed_homepage = urlparse(homepage_url)
    if parsed_candidate.scheme not in {"http", "https"}:
        return False
    host = (parsed_candidate.netloc or "").lower().split(":")[0]
    homepage_host = (parsed_homepage.netloc or "").lower().split(":")[0]
    allowed_hosts = {
        root_domain.lower(),
        f"www.{root_domain.lower()}",
        homepage_host,
    }
    return host in allowed_hosts


def extract_links_from_html(html: str) -> list[str]:
    parser = LinkExtractor()
    try:
        parser.feed(html)
    except Exception:
        return parser.links
    return parser.links


def extract_sitemaps_from_robots(base_url: str) -> list[str]:
    robots_url = urljoin(base_url if base_url.endswith("/") else f"{base_url}/", "robots.txt")
    try:
        final_url, body = fetch_text_response(robots_url, timeout_seconds=10)
    except Exception:
        return []
    sitemap_urls: list[str] = []
    for line in body.splitlines():
        if line.lower().startswith("sitemap:"):
            candidate = line.split(":", 1)[1].strip()
            if candidate:
                sitemap_urls.append(unescape(candidate))
    return [urljoin(final_url, item) if item.startswith("/") else unescape(item) for item in sitemap_urls]


def extract_urls_from_sitemap_xml(xml_text: str) -> tuple[list[str], list[str]]:
    sitemap_urls = re.findall(r"<sitemap>.*?<loc>(.*?)</loc>.*?</sitemap>", xml_text, flags=re.IGNORECASE | re.DOTALL)
    url_urls = re.findall(r"<url>.*?<loc>(.*?)</loc>.*?</url>", xml_text, flags=re.IGNORECASE | re.DOTALL)
    return [unescape(item.strip()) for item in sitemap_urls], [unescape(item.strip()) for item in url_urls]


def discover_sitemap_urls(final_homepage_url: str, root_domain: str) -> dict[str, Any]:
    parsed = urlparse(final_homepage_url)
    base = f"{parsed.scheme or 'https'}://{parsed.netloc}"
    candidates = extract_sitemaps_from_robots(base)
    source = "robots.txt" if candidates else ""
    if not candidates:
        candidates = [
            urljoin(base + "/", path)
            for path in ["sitemap.xml", "sitemap_index.xml", "sitemap-index.xml"]
        ]
        source = "common_paths"

    visited: set[str] = set()
    product_urls: list[str] = []
    category_urls: list[str] = []
    other_urls: list[str] = []
    winning_sitemap = ""

    def add_grouped(urls: list[str]) -> None:
        for url in urls:
            if not url_matches_same_site(url, final_homepage_url, root_domain):
                continue
            lowered = (urlparse(url).path or "").lower()
            if re.search(r"/(products?|product)(/|$)", lowered):
                product_urls.append(normalize_seed_url(url))
            elif re.search(r"/(collections?|categories?|category|shop)(/|$)", lowered):
                category_urls.append(normalize_seed_url(url))
            else:
                other_urls.append(normalize_seed_url(url))

    queue = list(candidates)
    while queue and len(visited) < 12:
        sitemap_url = queue.pop(0)
        if sitemap_url in visited:
            continue
        visited.add(sitemap_url)
        try:
            final_sitemap_url, body, encoding = fetch_binary_response(sitemap_url, timeout_seconds=12)
        except Exception:
            continue
        xml_text = body.decode(encoding, errors="replace")
        sitemap_children, url_children = extract_urls_from_sitemap_xml(xml_text)
        if url_children and not winning_sitemap:
            winning_sitemap = final_sitemap_url
        add_grouped(url_children)
        for child in sitemap_children[:8]:
            if child not in visited:
                queue.append(child)

    dedupe = lambda values: list(dict.fromkeys(values))
    return {
        "sitemap_found": 1 if (product_urls or category_urls or other_urls or winning_sitemap) else 0,
        "sitemap_url": winning_sitemap or (candidates[0] if candidates else ""),
        "sitemap_source": source,
        "product_urls": dedupe(product_urls),
        "category_urls": dedupe(category_urls),
        "other_urls": dedupe(other_urls),
    }


def fetch_text_response(url: str, *, timeout_seconds: int = 15) -> tuple[str, str]:
    request = Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            )
        },
    )
    with urlopen(request, timeout=timeout_seconds) as response:
        body = response.read()
        final_url = response.geturl()
        content_type = response.headers.get_content_charset() or "utf-8"
        return final_url, body.decode(content_type, errors="replace")


def fetch_binary_response(url: str, *, timeout_seconds: int = 15) -> tuple[str, bytes, str]:
    request = Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            )
        },
    )
    with urlopen(request, timeout=timeout_seconds) as response:
        return response.geturl(), response.read(), response.headers.get_content_charset() or "utf-8"


def classify_seed_urls(homepage_url: str, html: str, root_domain: str, profile: dict[str, Any]) -> dict[str, list[str]]:
    raw_links = extract_links_from_html(html)
    normalized_links: list[str] = []
    seen: set[str] = set()
    for raw_link in raw_links:
        absolute = urljoin(homepage_url, raw_link)
        if not url_matches_same_site(absolute, homepage_url, root_domain):
            continue
        normalized = normalize_seed_url(absolute)
        if normalized in seen:
            continue
        seen.add(normalized)
        normalized_links.append(normalized)

    grouped = {"category": [], "product": [], "service": [], "location": [], "other": []}
    pattern_map = {
        "category": [re.compile(pattern, re.IGNORECASE) for pattern in profile.get("category_patterns", [])],
        "product": [re.compile(pattern, re.IGNORECASE) for pattern in profile.get("product_patterns", [])],
        "service": [re.compile(pattern, re.IGNORECASE) for pattern in profile.get("service_patterns", [])],
        "location": [re.compile(pattern, re.IGNORECASE) for pattern in profile.get("location_patterns", [])],
    }

    for link in normalized_links:
        path = (urlparse(link).path or "/").lower()
        bucket = "other"
        for candidate_bucket, patterns in pattern_map.items():
            if any(pattern.search(path) for pattern in patterns):
                bucket = candidate_bucket
                break
        grouped[bucket].append(link)
    return grouped


def build_screamingfrog_seed_urls(root_domain: str, profile: dict[str, Any]) -> dict[str, Any]:
    platform_family = normalize_screamingfrog_platform_family(str(profile.get("family") or "generic"))
    homepage_url = f"https://{root_domain}"
    diagnostics = {
        "requested_homepage_url": homepage_url,
        "final_homepage_url": homepage_url,
        "redirect_detected": 0,
        "sitemap_found": 0,
        "sitemap_url": "",
        "sitemap_source": "",
        "seed_strategy": "homepage_only",
        "seed_count": 1,
        "seed_warning": "",
        "result_reason": "",
        "seeds": [homepage_url],
        "ranked_category_seeds": [],
    }
    try:
        final_url, html = fetch_text_response(homepage_url)
    except (HTTPError, URLError, TimeoutError, socket.timeout, ssl.SSLError) as exc:
        diagnostics["seed_warning"] = str(exc)
        diagnostics["result_reason"] = "homepage_fetch_failed"
        return diagnostics

    final_seed_url = normalize_seed_url(final_url)
    diagnostics["final_homepage_url"] = final_seed_url
    diagnostics["redirect_detected"] = 1 if normalized_url_key(final_url) != normalized_url_key(homepage_url) else 0

    max_category_urls = parse_int_value(profile.get("max_category_urls"))
    max_product_urls = parse_int_value(profile.get("max_product_urls"))
    max_service_urls = parse_int_value(profile.get("max_service_urls"))
    max_location_urls = parse_int_value(profile.get("max_location_urls"))
    max_other_urls = parse_int_value(profile.get("max_other_urls"))
    max_seed_urls = parse_int_value(profile.get("max_seed_urls"))

    sitemap = discover_sitemap_urls(final_seed_url, root_domain)
    diagnostics["sitemap_found"] = sitemap["sitemap_found"]
    diagnostics["sitemap_url"] = sitemap["sitemap_url"]
    diagnostics["sitemap_source"] = sitemap["sitemap_source"]
    ranked_category_urls, ranked_category_seed_data = rank_category_seed_urls(
        sitemap["category_urls"],
        platform_family=platform_family,
    )
    diagnostics["ranked_category_seeds"] = ranked_category_seed_data[:12]

    seeds = [final_seed_url]
    if sitemap["sitemap_found"]:
        seeds.extend(ranked_category_urls[:max_category_urls])
        seeds.extend(sitemap["product_urls"][:max_product_urls])
        seeds.extend(sitemap["other_urls"][:max_other_urls])
        diagnostics["seed_strategy"] = "sitemap"
    else:
        grouped = classify_seed_urls(final_seed_url, html, root_domain, profile)
        seeds.extend(grouped["category"][:max_category_urls])
        seeds.extend(grouped["product"][:max_product_urls])
        seeds.extend(grouped["service"][:max_service_urls])
        seeds.extend(grouped["location"][:max_location_urls])
        seeds.extend(grouped["other"][:max_other_urls])
        diagnostics["seed_strategy"] = "homepage_links" if len(seeds) > 1 else "homepage_only"

    deduped: list[str] = []
    seen: set[str] = set()
    for seed in seeds:
        normalized = normalize_seed_url(seed)
        if normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    diagnostics["seeds"] = deduped[:max_seed_urls or len(deduped)]
    diagnostics["seed_count"] = len(diagnostics["seeds"])
    if diagnostics["seed_count"] <= 1:
        diagnostics["result_reason"] = "redirect_only_homepage" if diagnostics["redirect_detected"] else "no_useful_seeds_found"
    elif not diagnostics["sitemap_found"] and diagnostics["seed_strategy"] == "homepage_links":
        diagnostics["result_reason"] = "homepage_links_only"
    return diagnostics


def screamingfrog_homepage_status_category(status_code: int) -> str:
    if status_code in {403, 429}:
        return "blocked"
    if 300 <= status_code <= 399:
        return "redirect"
    if 400 <= status_code <= 499:
        return "client_error"
    if 500 <= status_code <= 599:
        return "server_error"
    if 200 <= status_code <= 299:
        return "ok"
    return "other"


def collect_text_issue_flags(
    rows: list[dict[str, str]],
    text_key: str,
    length_key: str,
    *,
    min_length: int,
    max_length: int,
) -> list[str]:
    flags: list[str] = []
    values: list[str] = []
    has_missing = False
    too_short = False
    too_long = False
    for row in rows:
        text_value = (row.get(text_key) or "").strip()
        length_value = parse_int_value(row.get(length_key))
        if not text_value:
            has_missing = True
            continue
        values.append(text_value.lower())
        if 0 < length_value < min_length:
            too_short = True
        if length_value > max_length:
            too_long = True
    if has_missing:
        flags.append("missing")
    if len(values) != len(set(values)):
        flags.append("duplicate")
    if too_long:
        flags.append("too_long")
    if too_short:
        flags.append("too_short")
    return flags


def collect_canonical_issue_flags(rows: list[dict[str, str]]) -> list[str]:
    flags: list[str] = []
    has_missing = False
    has_non_indexable = False
    has_inconsistent = False
    for row in rows:
        address = parse_csv_address(row.get("Address"))
        canonical = (row.get("Canonical Link Element 1") or row.get("HTTP Canonical") or "").strip()
        indexability_status = (row.get("Indexability Status") or "").strip().lower()
        if not canonical:
            has_missing = True
        elif normalized_url_key(canonical) != normalized_url_key(address):
            has_inconsistent = True
        if "non-indexable" in indexability_status or "blocked by robots" in indexability_status:
            has_non_indexable = True
    if has_missing:
        flags.append("missing")
    if has_non_indexable:
        flags.append("non_indexable")
    if has_inconsistent:
        flags.append("inconsistent")
    return flags


def detect_location_page_count(rows: list[dict[str, str]]) -> int:
    pattern = re.compile(r"/(locations?|suburbs?|cities?|city|areas?|service-areas?|areas-we-serve)(/|$)", re.IGNORECASE)
    return sum(1 for row in rows if pattern.search(parse_csv_address(row.get("Address"))))


def detect_service_page_count(rows: list[dict[str, str]]) -> int:
    pattern = re.compile(r"/(services?|repairs?|installations?|consulting|treatments?|solutions)(/|$)", re.IGNORECASE)
    return sum(1 for row in rows if pattern.search(parse_csv_address(row.get("Address"))))


def detect_category_page_count(rows: list[dict[str, str]]) -> int:
    pattern = re.compile(r"/(collections?|categories?|category|shop)(/|$)", re.IGNORECASE)
    return sum(1 for row in rows if pattern.search(parse_csv_address(row.get("Address"))))


def detect_product_page_count(rows: list[dict[str, str]]) -> int:
    pattern = re.compile(r"/(products?|product)(/|$)", re.IGNORECASE)
    return sum(1 for row in rows if pattern.search(parse_csv_address(row.get("Address"))))


def load_collection_intelligence_rule_pack(platform_family: str) -> dict[str, Any]:
    normalized_family = normalize_screamingfrog_platform_family(platform_family)
    candidate_paths = [
        SCREAMINGFROG_COLLECTION_INTELLIGENCE_ROOT / f"{normalized_family}.json",
        SCREAMINGFROG_COLLECTION_INTELLIGENCE_ROOT / "generic.json",
    ]
    for path in candidate_paths:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    return {"family": "generic", "page_type": "collection", "fields": {}, "title_rules": {}}


def extract_site_brand(root_domain: str) -> str:
    host = root_domain.lower().split(":")[0]
    if host.startswith("www."):
        host = host[4:]
    parts = [part for part in host.split(".") if part]
    return parts[0] if parts else host


def normalize_collection_text(value: str) -> str:
    cleaned = re.sub(r"<[^>]+>", " ", value or "")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def classify_collection_page(address: str, platform_family: str) -> str:
    path = parse_csv_address(address).lower()
    if platform_family == "shopify":
        return "collection" if "/collections/" in path else "unknown"
    if re.search(r"/(collections?|categories?|category|shop)(/|$)", path):
        return "collection"
    return "unknown"


SHOPIFY_COLLECTION_EXACT_EXCLUSIONS = {
    "frontpage",
    "all",
    "vendors",
    "types",
}
SHOPIFY_COLLECTION_PARTIAL_EXCLUSIONS = (
    "test",
    "sample",
    "draft",
    "backup",
    "tmp",
    "dev",
)
CMS_COLLECTION_EXACT_EXCLUSIONS: dict[str, set[str]] = {
    "shopify": SHOPIFY_COLLECTION_EXACT_EXCLUSIONS,
    "woocommerce": {"shop", "uncategorized", "uncategorised"},
    "bigcommerce": {"all-products", "new-products", "featured-products"},
    "magento": {"all-products", "sale", "new", "catalog"},
    "neto": {"all", "featured", "specials"},
    "generic": {"shop", "catalog", "products", "all-products", "uncategorized", "uncategorised"},
}
CMS_COLLECTION_PARTIAL_EXCLUSIONS: dict[str, tuple[str, ...]] = {
    "shopify": SHOPIFY_COLLECTION_PARTIAL_EXCLUSIONS,
    "woocommerce": ("test", "sample", "draft", "backup", "uncategor"),
    "bigcommerce": ("test", "sample", "draft", "backup", "all-products", "featured-products"),
    "magento": ("test", "sample", "draft", "backup", "catalog", "all-products"),
    "neto": ("test", "sample", "draft", "backup", "featured", "specials"),
    "generic": ("test", "sample", "draft", "backup", "catalog", "all-products", "products"),
}
GENERIC_COLLECTION_NEGATIVE_TOKENS = (
    "products",
    "catalog",
    "archive",
    "archives",
    "misc",
    "uncategor",
    "frontpage",
)
GENERIC_COLLECTION_POSITIVE_TOKENS = (
    "accessories",
    "spares",
    "range",
    "wine",
    "monitor",
    "camera",
    "phone",
    "radio",
    "battery",
    "gear",
    "timber",
)


def collection_slug_from_address(address: str, platform_family: str) -> str:
    parsed = urlparse(parse_csv_address(address))
    path = parsed.path.strip("/").lower()
    segments = [segment for segment in path.split("/") if segment]
    if platform_family == "shopify":
        try:
            collection_index = segments.index("collections")
        except ValueError:
            collection_index = -1
        if collection_index >= 0 and collection_index + 1 < len(segments):
            return segments[collection_index + 1]
    return segments[-1] if segments else ""


def collection_candidate_exclusion_reason(address: str, platform_family: str) -> str:
    path = parse_csv_address(address).lower()
    slug = collection_slug_from_address(address, platform_family)
    exact_exclusions = CMS_COLLECTION_EXACT_EXCLUSIONS.get(platform_family, CMS_COLLECTION_EXACT_EXCLUSIONS["generic"])
    partial_exclusions = CMS_COLLECTION_PARTIAL_EXCLUSIONS.get(platform_family, CMS_COLLECTION_PARTIAL_EXCLUSIONS["generic"])
    if slug in exact_exclusions:
        return f"excluded_{slug}"
    if any(token in slug for token in partial_exclusions):
        return "excluded_test_or_temp_collection"
    if platform_family == "shopify":
        if path.endswith("/collections") or path.endswith("/collections/"):
            return "excluded_collection_index"
    if any(token in slug for token in ("search", "catalog")):
        return "excluded_generic_listing"
    return ""


def collection_seed_score(address: str, platform_family: str) -> tuple[int, list[str]]:
    path = parse_csv_address(address).lower()
    slug = collection_slug_from_address(address, platform_family)
    score = 0
    reasons: list[str] = []
    exclusion_reason = collection_candidate_exclusion_reason(address, platform_family)
    if exclusion_reason:
        return -999, [exclusion_reason]

    if slug:
        score += 12
        reasons.append("specific_slug")
    if any(token in slug for token in GENERIC_COLLECTION_NEGATIVE_TOKENS):
        score -= 22
        reasons.append("generic_bucket_slug")
    if re.search(r"(sale|clearance|gift|bundle|new|featured)", slug):
        score -= 6
        reasons.append("less_specific_merch_slug")
    if any(token in slug for token in GENERIC_COLLECTION_POSITIVE_TOKENS):
        score += 3
        reasons.append("commercial_collection_slug")
    if slug.count("-") >= 1:
        score += 4
        reasons.append("descriptive_slug")
    if re.search(r"/collections/", path):
        score += 8
        reasons.append("collection_path")
    if platform_family == "woocommerce":
        if "/product-category/" in path:
            score += 10
            reasons.append("product_category_path")
        if path.rstrip("/") == "/shop":
            score -= 30
            reasons.append("shop_archive_path")
    if platform_family == "bigcommerce":
        if "/categories/" in path:
            score += 10
            reasons.append("categories_path")
    if platform_family == "magento":
        if re.search(r"/category/", path):
            score += 10
            reasons.append("category_path")
        if "/catalog/" in path:
            score -= 18
            reasons.append("catalog_path")
    if platform_family == "neto":
        if re.search(r"/(category|categories|collections)/", path):
            score += 8
            reasons.append("category_path")
    if platform_family == "generic":
        if path.rstrip("/") == "/shop":
            score -= 30
            reasons.append("generic_shop_archive")
    return score, reasons


def rank_category_seed_urls(urls: list[str], *, platform_family: str) -> tuple[list[str], list[dict[str, Any]]]:
    ranked: list[dict[str, Any]] = []
    for url in urls:
        score, reasons = collection_seed_score(url, platform_family)
        if score <= -999:
            continue
        ranked.append({"url": normalize_seed_url(url), "score": score, "reasons": reasons})
    ranked.sort(key=lambda item: (-item["score"], item["url"]))
    ordered_urls = list(dict.fromkeys(item["url"] for item in ranked))
    return ordered_urls, ranked


def collection_candidate_score(
    row: dict[str, str],
    *,
    platform_family: str,
    title_row: dict[str, str] | None = None,
    h1_row: dict[str, str] | None = None,
    extracted_content: dict[str, str] | None = None,
) -> tuple[int, list[str]]:
    score = 0
    reasons: list[str] = []
    address = csv_row_value(row, "Address")
    slug = collection_slug_from_address(address, platform_family)
    title_value = normalize_collection_text(csv_row_value(title_row or row, "Title 1"))
    h1_value = normalize_collection_text(csv_row_value(h1_row or row, "H1-1"))
    lowered_title = title_value.lower()
    lowered_h1 = h1_value.lower()
    word_count = parse_int_value(csv_row_value(row, "Word Count"))
    status_code = parse_int_value(csv_row_value(row, "Status Code"))
    outlinks = parse_int_value(csv_row_value(row, "Outlinks")) or parse_int_value(csv_row_value(row, "Unique Outlinks"))
    extracted = extracted_content or {}
    above_text = normalize_collection_text(extracted.get("above_text") or "")
    below_text = normalize_collection_text(extracted.get("below_text") or "")
    content_presence = extracted.get("content_presence") or "unknown"

    if status_code and 200 <= status_code <= 299:
        score += 10
        reasons.append("status_200")
    elif status_code:
        score -= 12
        reasons.append("non_200_status")

    if slug and slug not in SHOPIFY_COLLECTION_EXACT_EXCLUSIONS:
        score += 10
        reasons.append("specific_collection_slug")
    if slug and any(token in slug for token in SHOPIFY_COLLECTION_PARTIAL_EXCLUSIONS):
        score -= 24
        reasons.append("temporary_or_test_slug")

    if title_value and "home page" not in lowered_title:
        score += 12
        reasons.append("non_generic_title")
    elif title_value:
        score -= 18
        reasons.append("generic_title")

    if h1_value and "home page" not in lowered_h1:
        score += 12
        reasons.append("non_generic_h1")
    elif h1_value:
        score -= 18
        reasons.append("generic_h1")
    else:
        score -= 8
        reasons.append("missing_h1")

    if above_text:
        score += 18
        reasons.append("above_grid_copy")
    elif below_text:
        score += 8
        reasons.append("below_grid_copy_only")
    else:
        score -= 10
        reasons.append("no_collection_copy")

    if content_presence == "content_above_and_below":
        score += 6
        reasons.append("content_above_and_below")
    elif content_presence == "content_above_only":
        score += 4
        reasons.append("content_above_only")

    if word_count >= 150:
        score += 6
        reasons.append("healthy_word_count")
    elif 0 < word_count < 60:
        score -= 4
        reasons.append("thin_word_count")

    if outlinks >= 8:
        score += 6
        reasons.append("likely_product_grid_links")
    elif outlinks == 0:
        score -= 6
        reasons.append("no_outlinks")

    if slug and re.search(r"(sale|clearance|new|featured)", slug):
        score -= 3
        reasons.append("less_specific_merch_slug")

    return score, reasons


def select_reviewable_collection_rows(
    internal_rows: list[dict[str, str]],
    *,
    platform_family: str,
    title_rows: list[dict[str, str]] | None = None,
    h1_rows: list[dict[str, str]] | None = None,
    max_rows: int = 3,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    def keyed(rows: list[dict[str, str]] | None) -> dict[str, dict[str, str]]:
        mapping: dict[str, dict[str, str]] = {}
        for row in rows or []:
            key = normalized_url_key(csv_row_value(row, "Address"))
            if key and key not in mapping:
                mapping[key] = row
        return mapping

    title_map = keyed(title_rows)
    h1_map = keyed(h1_rows)
    candidates: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []

    for row in internal_rows:
        address = csv_row_value(row, "Address")
        if classify_collection_page(address, platform_family) != "collection":
            continue
        exclusion_reason = collection_candidate_exclusion_reason(address, platform_family)
        if exclusion_reason:
            excluded.append({"row": row, "reason": exclusion_reason, "score": -999, "selection_status": "exclude"})
            continue
        extracted = extract_collection_page_content(address) if address else {
            "above_raw_text": "",
            "below_raw_text": "",
            "above_text": "",
            "below_text": "",
            "content_presence": "unknown",
            "error": "",
        }
        key = normalized_url_key(address)
        title_row = title_map.get(key, {})
        h1_row = h1_map.get(key, {})
        score, reasons = collection_candidate_score(
            row,
            platform_family=platform_family,
            title_row=title_row,
            h1_row=h1_row,
            extracted_content=extracted,
        )
        selection_status = "reviewable" if score >= 20 else "weak"
        candidates.append(
            {
                "row": row,
                "title_row": title_row,
                "h1_row": h1_row,
                "extracted_content": extracted,
                "score": score,
                "reasons": reasons,
                "selection_status": selection_status,
            }
        )

    ranked = sorted(
        candidates,
        key=lambda item: (
            0 if item["selection_status"] == "reviewable" else 1,
            -item["score"],
            parse_csv_address(csv_row_value(item["row"], "Address")),
        ),
    )
    selected = ranked[:max_rows]
    return selected, excluded


def classify_collection_intro(text: str, *, position: str, product_count: int) -> tuple[str, int]:
    normalized = normalize_collection_text(text)
    if not normalized:
        return "missing_intro", 10
    word_count = len(normalized.split())
    lowered = normalized.lower()
    if position == "below_grid":
        return "below_grid_copy", 42
    if word_count < 20:
        return "light_intro", 56
    if any(token in lowered for token in ["shipping", "returns", "newsletter", "subscribe"]):
        return "boilerplate_intro", 38
    if product_count and word_count >= 20 and position == "above_grid":
        return "strong_intro", 84
    return "light_intro", 58


COLLECTION_UI_IDENTIFIER_TOKENS = (
    "breadcrumb",
    "breadcrumbs",
    "filter",
    "filters",
    "facet",
    "facets",
    "toolbar",
    "sort",
    "pagination",
    "breadcrumbs",
    "product-grid",
    "product-card",
    "grid__item",
    "collection-toolbar",
    "count",
)

COLLECTION_UI_TEXT_PATTERNS = (
    "sort by",
    "featured",
    "best selling",
    "alphabetically",
    "price, low to high",
    "price, high to low",
    "date, old to new",
    "date, new to old",
    "sale price",
    "regular price",
    "add to cart",
    "remove all",
    "filter",
    "filters",
    "showing",
    "products",
    "product count",
    "load more",
    "view all",
    "quick add",
    "columns",
    "list",
)

COLLECTION_SUPPORT_TEXT_PATTERNS = (
    "shipping",
    "returns",
    "newsletter",
    "subscribe",
    "privacy policy",
    "terms and conditions",
    "customer service",
    "contact us",
)


def text_sentence_like_ratio(text: str) -> float:
    sentences = re.split(r"(?<=[.!?])\s+", text.strip())
    if not sentences:
        return 0.0
    sentence_like = sum(1 for sentence in sentences if len(sentence.split()) >= 6)
    return sentence_like / max(len(sentences), 1)


def layout_boilerplate_ratio(text: str) -> float:
    tokens = [token for token in re.findall(r"[a-z0-9]+", text.lower()) if token]
    if not tokens:
        return 0.0
    repeated_pairs = 0
    for index in range(0, max(len(tokens) - 1, 0), 2):
        pair = tokens[index:index + 2]
        if len(pair) == 2 and pair[0] == pair[1]:
            repeated_pairs += 1
    unique_ratio = len(set(tokens)) / max(len(tokens), 1)
    repeated_ratio = repeated_pairs / max(len(tokens) // 2, 1)
    return max(repeated_ratio, 1 - unique_ratio)


def classify_collection_text_type(text: str, identifier: str) -> str:
    lowered = text.lower()
    identifier_lower = identifier.lower()
    if layout_boilerplate_ratio(text) >= 0.45:
        return "ui_controls"
    if any(token in identifier_lower for token in COLLECTION_UI_IDENTIFIER_TOKENS):
        return "ui_controls"
    if any(pattern in lowered for pattern in COLLECTION_UI_TEXT_PATTERNS):
        return "ui_controls"
    if any(pattern in lowered for pattern in ("colour", "size", "brand", "availability", "price range")):
        return "facet_text"
    if any(pattern in lowered for pattern in ("add to cart", "quick add", "sale price", "regular price")):
        return "product_card_text"
    if any(pattern in lowered for pattern in COLLECTION_SUPPORT_TEXT_PATTERNS):
        return "footer_support"
    if len(text.split()) < 14:
        return "mixed"
    if text_sentence_like_ratio(text) >= 0.45:
        return "intro_copy"
    return "mixed"


def score_collection_text_block(text: str, *, region: str, source_type: str) -> tuple[int, int]:
    word_count = len(text.split())
    confidence = 20
    score = 0

    if source_type == "intro_copy":
        score += 28
        confidence += 38
    elif source_type in {"ui_controls", "facet_text", "product_card_text"}:
        score -= 28
        confidence -= 8
    elif source_type == "footer_support":
        score -= 18
        confidence += 4
    else:
        score += 4
        confidence += 8

    if region == "above_grid":
        score += 16
        confidence += 18
    elif region == "near_grid":
        score += 8
        confidence += 12
    elif region == "below_grid":
        score += 2
        confidence += 6

    if word_count >= 45:
        score += 16
        confidence += 16
    elif word_count >= 24:
        score += 9
        confidence += 10
    elif word_count < 12:
        score -= 16
        confidence -= 10

    if text_sentence_like_ratio(text) >= 0.45:
        score += 10
        confidence += 10
    if layout_boilerplate_ratio(text) >= 0.45:
        score -= 24
        confidence -= 14

    return score, max(0, min(confidence, 100))


@lru_cache(maxsize=256)
def fetch_collection_page_html(url: str) -> str:
    request = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
        },
    )
    with urlopen(request, timeout=6) as response:
        return response.read().decode("utf-8", "ignore")


def detect_collection_grid_node(main_node: Tag | None) -> Tag | None:
    if main_node is None:
        return None
    selectors = [
        "[id*='product-grid']",
        "[class*='product-grid']",
        "[id*='collection-products']",
        "[class*='collection-products']",
        "ul.product-grid",
        ".product-grid-container",
    ]
    for selector in selectors:
        node = main_node.select_one(selector)
        if node is not None:
            return node
    return None


def collection_text_blocks(main_node: Tag | None, grid_node: Tag | None) -> list[dict[str, Any]]:
    if main_node is None:
        return []
    for removable in main_node.find_all(["script", "style", "noscript", "svg", "template"]):
        removable.decompose()
    ordered_nodes = list(main_node.find_all(True))
    order_index = {id(node): index for index, node in enumerate(ordered_nodes)}
    grid_index = order_index.get(id(grid_node), 10**9) if grid_node is not None else 10**9

    blocks: list[dict[str, Any]] = []
    seen: set[str] = set()
    for node in ordered_nodes:
        if node.name not in {"div", "section", "article", "p"}:
            continue
        classes = " ".join(node.get("class", []))
        identifier = f"{classes} {node.get('id', '')}".lower()
        text = normalize_collection_text(" ".join(node.stripped_strings))
        word_count = len(text.split())
        if word_count < 8 or word_count > 220:
            continue
        if text in seen:
            continue
        seen.add(text)
        node_index = order_index.get(id(node), 10**9)
        if node_index < grid_index:
            region = "above_grid"
        elif node_index <= grid_index + 20:
            region = "near_grid"
        else:
            region = "below_grid"
        source_type = classify_collection_text_type(text, identifier)
        score, confidence = score_collection_text_block(text, region=region, source_type=source_type)
        blocks.append(
            {
                "text": text,
                "region": region,
                "source_type": source_type,
                "score": score,
                "confidence": confidence,
                "identifier": identifier,
            }
        )
    return blocks


def clean_collection_content_text(text: str) -> str:
    normalized = normalize_collection_text(text)
    if not normalized:
        return ""
    cleaned = normalized
    cleanup_patterns = [
        r"Home\s*/\s*Our Brands\s*/\s*[^.]+",
        r"Home\s*/\s*All Products",
        r"\b\d+\s+Products?\b",
        r"Sort By:\s*Featured.*?(?=(?:[A-Z][a-z]|$))",
        r"\bFeatured Most relevant Best selling Alphabetically, A-Z Alphabetically, Z-A Price, low to high Price, high to low Date, old to new Date, new to old\b",
        r"\bRemove all\b",
    ]
    for pattern in cleanup_patterns:
        cleaned = re.sub(pattern, " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    sentences = re.split(r"(?<=[.!?])\s+", cleaned)
    useful_sentences: list[str] = []
    seen: set[str] = set()
    for sentence in sentences:
        trimmed = sentence.strip(" -")
        lowered = trimmed.lower()
        if len(trimmed.split()) < 6:
            continue
        if any(token in lowered for token in ["regular price", "sale price", "add to cart", "view ", "sort by", "best selling", "alphabetically"]):
            continue
        if lowered in seen:
            continue
        seen.add(lowered)
        useful_sentences.append(trimmed)
    return " ".join(useful_sentences[:5]).strip()


def build_collection_content_summary(blocks: list[dict[str, Any]]) -> dict[str, Any]:
    filtered_blocks = [block for block in blocks if block.get("source_type") not in {"ui_controls", "facet_text", "product_card_text"}]
    ranked_blocks = sorted(filtered_blocks, key=lambda item: (-int(item.get("score") or 0), -int(item.get("confidence") or 0), item.get("text", "")))
    above_blocks = [block for block in ranked_blocks if block.get("region") in {"above_grid", "near_grid"}]
    below_blocks = [block for block in ranked_blocks if block.get("region") == "below_grid"]

    above_raw_text = "\n\n".join(block["text"] for block in above_blocks[:3]).strip()
    below_raw_text = "\n\n".join(block["text"] for block in below_blocks[:3]).strip()
    above_clean_text = clean_collection_content_text(above_raw_text)
    below_clean_text = clean_collection_content_text(below_raw_text)

    best_block = ranked_blocks[0] if ranked_blocks else None
    best_text = clean_collection_content_text(str(best_block.get("text") or "")) if best_block else ""
    best_position = str(best_block.get("region") or "unknown") if best_block else "unknown"
    best_source_type = str(best_block.get("source_type") or "unknown") if best_block else "unknown"
    best_confidence = int(best_block.get("confidence") or 0) if best_block else 0

    ui_heavy_count = sum(1 for block in blocks if block.get("source_type") in {"ui_controls", "facet_text", "product_card_text"})
    prose_count = sum(1 for block in ranked_blocks if block.get("source_type") == "intro_copy")
    total_blocks = len(blocks)

    if above_clean_text and best_position in {"above_grid", "near_grid"} and best_source_type == "intro_copy" and best_confidence >= 75:
        intro_status = "strong_intro"
        intro_confidence = best_confidence
        intro_text = above_clean_text
        intro_position = "above_grid" if best_position == "above_grid" else "near_grid"
    elif above_clean_text and best_confidence >= 55:
        intro_status = "light_intro"
        intro_confidence = best_confidence
        intro_text = above_clean_text
        intro_position = "above_grid" if best_position == "above_grid" else best_position
    elif below_clean_text and best_confidence >= 55:
        intro_status = "below_grid_content"
        intro_confidence = best_confidence
        intro_text = below_clean_text
        intro_position = "below_grid"
    elif not above_clean_text and not below_clean_text and total_blocks and ui_heavy_count >= max(1, total_blocks // 2):
        intro_status = "boilerplate_only"
        intro_confidence = 72
        intro_text = ""
        intro_position = "unknown"
    elif best_text and best_confidence < 55:
        intro_status = "mixed_low_confidence"
        intro_confidence = best_confidence
        intro_text = best_text
        intro_position = best_position
    elif not best_text and prose_count == 0 and total_blocks:
        intro_status = "boilerplate_only"
        intro_confidence = 68
        intro_text = ""
        intro_position = "unknown"
    elif not best_text:
        intro_status = "missing_intro"
        intro_confidence = 12
        intro_text = ""
        intro_position = "unknown"
    else:
        intro_status = "unknown"
        intro_confidence = best_confidence
        intro_text = best_text
        intro_position = best_position

    if above_clean_text and below_clean_text:
        content_presence = "content_above_and_below"
    elif above_clean_text:
        content_presence = "content_above_only"
    elif below_clean_text:
        content_presence = "content_below_only"
    else:
        content_presence = "no_content"

    return {
        "above_raw_text": above_raw_text,
        "below_raw_text": below_raw_text,
        "above_text": above_clean_text,
        "below_text": below_clean_text,
        "best_intro_text": intro_text,
        "best_intro_position": intro_position,
        "best_intro_confidence": intro_confidence,
        "best_intro_source_type": best_source_type,
        "content_presence": content_presence,
        "intro_status": intro_status,
        "ui_heavy_count": ui_heavy_count,
        "prose_count": prose_count,
        "candidate_count": total_blocks,
    }


def extract_collection_page_content(url: str) -> dict[str, str]:
    try:
        html = fetch_collection_page_html(url)
    except Exception as exc:
        return {
            "above_text": "",
            "below_text": "",
            "content_presence": "unknown",
            "error": str(exc),
        }
    soup = BeautifulSoup(html, "html.parser")
    main_node = soup.find("main")
    grid_node = detect_collection_grid_node(main_node if isinstance(main_node, Tag) else None)
    blocks = collection_text_blocks(main_node if isinstance(main_node, Tag) else None, grid_node)
    summary = build_collection_content_summary(blocks)
    return {
        "above_raw_text": summary["above_raw_text"],
        "below_raw_text": summary["below_raw_text"],
        "above_text": summary["above_text"],
        "below_text": summary["below_text"],
        "best_intro_text": summary["best_intro_text"],
        "best_intro_position": summary["best_intro_position"],
        "best_intro_confidence": summary["best_intro_confidence"],
        "best_intro_source_type": summary["best_intro_source_type"],
        "intro_status": summary["intro_status"],
        "content_presence": summary["content_presence"],
        "ui_heavy_count": summary["ui_heavy_count"],
        "prose_count": summary["prose_count"],
        "candidate_count": summary["candidate_count"],
        "error": "",
    }


HEADING_TAG_SEQUENCE = ("h1", "h2", "h3", "h4", "h5", "h6")
GENERIC_HEADING_TOKENS = {
    "home",
    "home page",
    "shop",
    "products",
    "product",
    "catalog",
    "category",
    "categories",
    "collection",
    "frontpage",
    "discover your product solution",
}


def resolve_heading_root(soup: BeautifulSoup) -> Tag:
    return soup.find("main") or soup.find(attrs={"role": "main"}) or soup.body or soup


def heading_text_is_generic(text: str) -> bool:
    normalized = normalize_collection_text(text).strip().lower().strip(":|- ")
    if not normalized:
        return False
    if normalized in GENERIC_HEADING_TOKENS:
        return True
    if normalized.startswith("collection:"):
        candidate = normalized.replace("collection:", "", 1).strip()
        if candidate in GENERIC_HEADING_TOKENS:
            return True
    if normalized.startswith("page ") and len(normalized.split()) <= 3:
        return True
    return False


@lru_cache(maxsize=512)
def extract_page_heading_outline(url: str) -> dict[str, Any]:
    try:
        html = fetch_collection_page_html(url)
    except Exception as exc:  # noqa: BLE001
        return {
            "error": str(exc),
            "headings": [],
            "counts": {tag: 0 for tag in HEADING_TAG_SEQUENCE},
            "texts_by_tag": {tag: [] for tag in HEADING_TAG_SEQUENCE},
            "generic_heading_count": 0,
            "duplicate_text_count": 0,
        }

    soup = BeautifulSoup(html, "html.parser")
    root = resolve_heading_root(soup)
    for removable in root.find_all(["script", "style", "noscript", "svg", "template"]):
        removable.decompose()

    headings: list[dict[str, str]] = []
    texts_by_tag: dict[str, list[str]] = {tag: [] for tag in HEADING_TAG_SEQUENCE}
    for node in root.find_all(list(HEADING_TAG_SEQUENCE)):
        text = normalize_collection_text(" ".join(node.stripped_strings))
        if not text or len(text.split()) > 28:
            continue
        tag = node.name.lower()
        headings.append({"tag": tag, "text": text})
        texts_by_tag[tag].append(text)

    counts = {tag: len(texts_by_tag[tag]) for tag in HEADING_TAG_SEQUENCE}
    normalized_texts = [item["text"].strip().lower() for item in headings if item["text"].strip()]
    duplicate_text_count = sum(1 for count in Counter(normalized_texts).values() if count > 1)
    generic_heading_count = sum(1 for item in headings if heading_text_is_generic(item["text"]))
    return {
        "error": "",
        "headings": headings,
        "counts": counts,
        "texts_by_tag": texts_by_tag,
        "generic_heading_count": generic_heading_count,
        "duplicate_text_count": duplicate_text_count,
    }


def build_heading_intelligence(internal_rows: list[dict[str, str]]) -> dict[str, Any]:
    page_candidates: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    for row in internal_rows:
        address = parse_csv_address(csv_row_value(row, "Address"))
        if not address or address in seen_urls:
            continue
        seen_urls.add(address)
        status_code = parse_int_value(csv_row_value(row, "Status Code"))
        if status_code and not (200 <= status_code <= 299):
            continue
        page_candidates.append({"url": address, "word_count": parse_int_value(csv_row_value(row, "Word Count"))})
        if len(page_candidates) >= 12:
            break

    pages_analyzed = 0
    h1_missing_count = 0
    multiple_h1_count = 0
    pages_with_h2_count = 0
    generic_heading_count = 0
    repeated_heading_count = 0
    h1_to_urls: dict[str, list[str]] = defaultdict(list)
    page_rows: list[dict[str, Any]] = []

    for candidate in page_candidates:
        outline = extract_page_heading_outline(candidate["url"])
        texts_by_tag = outline["texts_by_tag"]
        counts = outline["counts"]
        h1s = texts_by_tag["h1"]
        h2s = texts_by_tag["h2"]
        pages_analyzed += 1

        if not h1s:
            h1_missing_count += 1
        if counts["h1"] > 1:
            multiple_h1_count += 1
        if h2s:
            pages_with_h2_count += 1
        if outline["generic_heading_count"] > 0:
            generic_heading_count += 1
        if outline["duplicate_text_count"] > 0:
            repeated_heading_count += 1
        if h1s:
            h1_to_urls[normalize_collection_text(h1s[0]).lower()].append(candidate["url"])

        page_issue_flags: list[str] = []
        if not h1s:
            page_issue_flags.append("Missing H1")
        if counts["h1"] > 1:
            page_issue_flags.append("Multiple H1s")
        if candidate["word_count"] >= 250 and not h2s:
            page_issue_flags.append("No H2 structure")
        if outline["generic_heading_count"] > 0:
            page_issue_flags.append("Generic headings")
        if outline["duplicate_text_count"] > 0:
            page_issue_flags.append("Repeated headings")

        page_rows.append(
            {
                "URL": candidate["url"],
                "Issue": " | ".join(page_issue_flags) if page_issue_flags else "Healthy",
                "H1": " | ".join(h1s) if h1s else "—",
                "H2": " | ".join(h2s[:5]) if h2s else "—",
                "H3": " | ".join(texts_by_tag["h3"][:5]) if texts_by_tag["h3"] else "—",
                "H4-H6": " | ".join((texts_by_tag["h4"] + texts_by_tag["h5"] + texts_by_tag["h6"])[:6]) or "—",
                "All headings": " | ".join(item["text"] for item in outline["headings"][:16]) or "—",
            }
        )

    duplicate_h1_count = sum(1 for value, urls in h1_to_urls.items() if value and len(urls) > 1)
    heading_issue_flags: list[str] = []
    heading_outline_score = 0
    summary_parts: list[str] = []

    if pages_analyzed:
        summary_parts.append(f"{pages_analyzed} pages analysed")
    if h1_missing_count:
        heading_issue_flags.append("missing_h1")
        heading_outline_score += min(12, h1_missing_count * 4)
        summary_parts.append(f"{h1_missing_count} missing H1")
    if multiple_h1_count:
        heading_issue_flags.append("multiple_h1")
        heading_outline_score += min(8, multiple_h1_count * 3)
        summary_parts.append(f"{multiple_h1_count} pages with multiple H1s")
    if duplicate_h1_count:
        heading_issue_flags.append("duplicate_h1_across_pages")
        heading_outline_score += min(8, duplicate_h1_count * 3)
        summary_parts.append(f"{duplicate_h1_count} duplicated H1 patterns")
    if pages_analyzed and pages_with_h2_count < pages_analyzed:
        heading_issue_flags.append("weak_heading_depth")
        heading_outline_score += 6
        summary_parts.append(f"{pages_analyzed - pages_with_h2_count} pages without H2 support")
    if generic_heading_count:
        heading_issue_flags.append("generic_heading_patterns")
        heading_outline_score += min(6, generic_heading_count * 2)
        summary_parts.append(f"{generic_heading_count} pages with generic headings")
    if repeated_heading_count:
        heading_issue_flags.append("repeated_heading_text")
        heading_outline_score += min(6, repeated_heading_count * 2)
        summary_parts.append(f"{repeated_heading_count} pages with repeated heading text")

    return {
        "heading_issue_flags": "|".join(dict.fromkeys(heading_issue_flags)),
        "heading_outline_score": heading_outline_score,
        "heading_outline_summary": " | ".join(summary_parts),
        "heading_pages_analyzed": pages_analyzed,
        "heading_h1_missing_count": h1_missing_count,
        "heading_multiple_h1_count": multiple_h1_count,
        "heading_duplicate_h1_count": duplicate_h1_count,
        "heading_pages_with_h2_count": pages_with_h2_count,
        "heading_generic_heading_count": generic_heading_count,
        "heading_repeated_heading_count": repeated_heading_count,
        "page_rows": page_rows,
    }


def collection_issue_reason_for_status(status: str) -> str:
    mapping = {
        "missing_intro": "No collection copy was extracted above or below the product grid",
        "light_intro": "Collection copy exists but looks very light for a commercial category page",
        "below_grid_copy": "Collection copy was found below the product grid rather than as an intro block",
        "below_grid_content": "Useful collection copy was found below the product grid",
        "boilerplate_intro": "Collection copy looks boilerplate rather than category-specific",
        "boilerplate_only": "Only boilerplate or UI-heavy collection copy was extracted",
        "mixed_low_confidence": "Collection copy was found, but the extraction is too mixed to trust strongly",
        "strong_intro": "Collection intro copy is present and substantial",
        "unknown": "The saved crawl did not capture a reliable collection intro read",
    }
    return mapping.get(status, "")


def classify_crawl_evidence_grade(payload: Mapping[str, Any]) -> tuple[str, str]:
    status = str(payload.get("status") or "").strip()
    result_quality = str(payload.get("result_quality") or "").strip()
    result_reason = str(payload.get("result_reason") or "").strip()
    pages_crawled = int(payload.get("pages_crawled") or 0)
    seed_count = int(payload.get("seed_count") or 0)
    category_page_count = int(payload.get("category_page_count") or 0)
    product_page_count = int(payload.get("product_page_count") or 0)
    collection_detection_status = str(payload.get("collection_detection_status") or "").strip()
    collection_detection_confidence = int(payload.get("collection_detection_confidence") or 0)
    seed_strategy = str(payload.get("seed_strategy") or "").strip()

    if status == "error":
        return "F", "crawl failed"
    if result_quality == "error":
        return "F", "crawl error"
    if result_reason in {"no_useful_seeds_found", "redirect_only_homepage", "homepage_fetch_failed"}:
        return "D", "thin crawl evidence"
    if pages_crawled <= 1 or seed_count <= 1:
        return "D", "homepage-only evidence"
    if category_page_count >= 5 and pages_crawled >= 7 and collection_detection_status == "detected" and collection_detection_confidence >= 80:
        return "A", "strong category coverage"
    if category_page_count >= 3 and pages_crawled >= 5 and collection_detection_confidence >= 70:
        return "B", "good category coverage"
    if pages_crawled >= 3 and (category_page_count >= 1 or product_page_count >= 1 or seed_strategy == "sitemap"):
        return "C", "usable but limited coverage"
    if result_quality in {"partial", "weak"}:
        return "D", "weak crawl evidence"
    return "C", "limited crawl evidence"


def build_title_rule_variants(patterns: Sequence[str], replacements: Mapping[str, str]) -> list[str]:
    values: list[str] = []
    for pattern in patterns:
        values.append(pattern.lower().format_map(defaultdict(str, replacements)).strip())
    return [value for value in values if value]


def classify_title_optimization_status(
    title: str,
    h1: str,
    root_domain: str,
    platform_family: str,
    title_rules: Mapping[str, Any],
) -> tuple[str, int, str, int]:
    normalized_title = normalize_collection_text(title).lower()
    normalized_h1 = normalize_collection_text(h1).lower()
    if not normalized_title or not normalized_h1:
        return "unknown", 0, "", 0
    brand = extract_site_brand(root_domain).replace("-", " ").replace("_", " ").lower()
    site_candidates = {root_domain.lower(), brand, brand.title().lower()}
    replacements = {"term": normalized_h1, "site": brand, "brand": brand}
    exact_patterns = build_title_rule_variants(title_rules.get("default_exact", []), replacements)
    like_patterns = build_title_rule_variants(title_rules.get("default_like", []), replacements)
    term_plus_site_patterns = build_title_rule_variants(title_rules.get("term_plus_site", []), replacements)
    safe_customised_patterns = build_title_rule_variants(title_rules.get("safe_customised", []), replacements)
    site_name_match = 1 if any(site and site in normalized_title for site in site_candidates) else 0

    if normalized_title in exact_patterns or normalized_title == normalized_h1:
        return "default_exact", 96, normalized_title, site_name_match
    if normalized_title in safe_customised_patterns:
        return "customised", 88, normalized_title, site_name_match
    if normalized_title in term_plus_site_patterns:
        return "term_plus_site", 66, normalized_title, site_name_match
    if normalized_title in like_patterns:
        return "default_like", 82, normalized_title, site_name_match
    if normalized_h1 in normalized_title and site_name_match:
        if platform_family in {"shopify", "woocommerce", "magento", "generic"}:
            return "term_plus_site", 60, normalized_title, site_name_match
    if normalized_h1 in normalized_title:
        if len(normalized_title.split()) >= len(normalized_h1.split()) + 2:
            return "customised", 74, normalized_title, site_name_match
        return "unknown", 40, normalized_title, site_name_match
    return "unknown", 28, normalized_title, site_name_match


def build_collection_intelligence(payload: dict[str, Any], *, root_domain: str, platform_family: str, internal_rows: list[dict[str, str]], title_rows: list[dict[str, str]], h1_rows: list[dict[str, str]], structured_rows: list[dict[str, str]], directive_rows: list[dict[str, str]]) -> dict[str, Any]:
    rule_pack = load_collection_intelligence_rule_pack(platform_family)
    selected_rows, excluded_rows = select_reviewable_collection_rows(
        internal_rows,
        platform_family=platform_family,
        title_rows=title_rows,
        h1_rows=h1_rows,
        max_rows=5,
    )
    reviewable_rows = [item for item in selected_rows if item.get("selection_status") == "reviewable"]
    collection_rows = [item["row"] for item in reviewable_rows]
    collection_detection_status = "detected" if collection_rows else ("weak_detection" if selected_rows else "unknown")
    collection_detection_confidence = max((item.get("confidence", 0) for item in rule_pack.get("collection_detection", [])), default=0) if collection_rows else 18
    selected_candidate = reviewable_rows[0] if reviewable_rows else {}
    homepage_row = selected_candidate.get("row", {})
    collection_url = parse_csv_address(homepage_row.get("Address"))
    title_row = selected_candidate.get("title_row") or next((row for row in title_rows if normalized_url_key(row.get("Address")) == normalized_url_key(homepage_row.get("Address"))), title_rows[0] if title_rows else {})
    h1_row = selected_candidate.get("h1_row") or next((row for row in h1_rows if normalized_url_key(row.get("Address")) == normalized_url_key(homepage_row.get("Address"))), h1_rows[0] if h1_rows else {})
    structured_row = next((row for row in structured_rows if normalized_url_key(row.get("Address")) == normalized_url_key(homepage_row.get("Address"))), structured_rows[0] if structured_rows else {})
    directives_row = next((row for row in directive_rows if normalized_url_key(row.get("Address")) == normalized_url_key(homepage_row.get("Address"))), directive_rows[0] if directive_rows else {})

    if not reviewable_rows or not collection_url:
        return {
            "collection_detection_status": collection_detection_status,
            "collection_detection_confidence": collection_detection_confidence,
            "collection_main_content": "",
            "collection_main_content_method": "not_reviewable_collection_page",
            "collection_main_content_confidence": 0,
            "collection_above_raw_text": "",
            "collection_below_raw_text": "",
            "collection_above_clean_text": "",
            "collection_below_clean_text": "",
            "collection_best_intro_text": "",
            "collection_best_intro_position": "unknown",
            "collection_best_intro_confidence": 0,
            "collection_best_intro_source_type": "none",
            "collection_intro_text": "",
            "collection_intro_position": "unknown",
            "collection_intro_status": "unknown",
            "collection_intro_method": "not_reviewable_collection_page",
            "collection_intro_confidence": 0,
            "collection_schema_types": "",
            "collection_schema_types_method": "none",
            "collection_schema_types_confidence": 0,
            "collection_product_count": 0,
            "collection_product_count_method": "none",
            "collection_product_count_confidence": 0,
            "collection_title_value": "",
            "collection_title_method": "not_reviewable_collection_page",
            "collection_title_confidence": 0,
            "collection_h1_value": "",
            "collection_h1_method": "not_reviewable_collection_page",
            "collection_h1_confidence": 0,
            "title_optimization_status": "unknown",
            "title_optimization_confidence": 0,
            "collection_title_rule_family": platform_family,
            "collection_title_rule_match": "",
            "collection_title_rule_confidence": 0,
            "collection_title_site_name_match": 0,
            "collection_issue_family": "collection_page_not_reviewable",
            "collection_issue_reason": "No reliable collection/category page was captured in the crawl",
            "collection_review_url": "",
            "collection_review_score": 0,
            "collection_review_status": "not_reviewable",
        }

    extracted_content = selected_candidate.get("extracted_content") or (
        extract_collection_page_content(collection_url)
        if collection_url
        else {
            "above_raw_text": "",
            "below_raw_text": "",
            "above_text": "",
            "below_text": "",
            "best_intro_text": "",
            "best_intro_position": "unknown",
            "best_intro_confidence": 0,
            "best_intro_source_type": "unknown",
            "intro_status": "unknown",
            "content_presence": "unknown",
            "error": "",
        }
    )
    above_grid_text = normalize_collection_text(extracted_content.get("above_text") or "")
    below_grid_text = normalize_collection_text(extracted_content.get("below_text") or "")
    above_raw_text = normalize_collection_text(extracted_content.get("above_raw_text") or "")
    below_raw_text = normalize_collection_text(extracted_content.get("below_raw_text") or "")
    best_intro_text = normalize_collection_text(extracted_content.get("best_intro_text") or "")
    best_intro_position = str(extracted_content.get("best_intro_position") or "unknown")
    best_intro_confidence = int(extracted_content.get("best_intro_confidence") or 0)
    best_intro_source_type = str(extracted_content.get("best_intro_source_type") or "unknown")
    main_content = above_grid_text or below_grid_text or best_intro_text
    main_content_method = "live_collection_html" if main_content else ("fetch_error" if extracted_content.get("error") else "missing")
    main_content_confidence = best_intro_confidence if main_content else 12

    intro_text = best_intro_text
    intro_position = best_intro_position
    intro_status = str(extracted_content.get("intro_status") or "unknown")
    intro_confidence = best_intro_confidence
    intro_method = "live_collection_html" if (above_grid_text or below_grid_text) else ("fetch_error" if extracted_content.get("error") else "missing")

    schema_types = [row.get("Type-1", "").strip() for row in structured_rows if row.get("Type-1", "").strip()]
    unique_schema_types = list(dict.fromkeys(schema_types))
    schema_types_text = " | ".join(unique_schema_types)
    schema_types_method = "structured_data_csv" if unique_schema_types else "none"
    schema_types_confidence = 86 if unique_schema_types else 18

    product_count = int(payload.get("product_page_count") or 0)
    product_count_method = "url_pattern_count" if product_count else "none"
    product_count_confidence = 66 if product_count else 20

    title_value = normalize_collection_text(title_row.get("Title 1") or homepage_row.get("Title 1") or payload.get("homepage_title") or "")
    title_method = "page_titles_csv" if title_value else "missing"
    title_confidence = 95 if title_value else 10

    h1_value = normalize_collection_text(h1_row.get("H1-1") or "")
    h1_method = "h1_csv" if h1_value else "missing"
    h1_confidence = 92 if h1_value else 10

    title_optimization_status, title_optimization_confidence, title_rule_match, title_site_name_match = classify_title_optimization_status(
        title_value,
        h1_value,
        root_domain,
        platform_family,
        rule_pack.get("title_rules", {}),
    )

    collection_issue_family = ""
    collection_issue_reason = collection_issue_reason_for_status(intro_status)
    if intro_status == "missing_intro" and intro_confidence >= 70:
        collection_issue_family = "collection_content_gap"
    elif intro_status in {"boilerplate_only", "mixed_low_confidence"}:
        collection_issue_family = "collection_content_unclear"
        collection_issue_reason = collection_issue_reason_for_status(intro_status)
    elif title_optimization_status == "default_exact":
        collection_issue_family = "default_collection_title"
        collection_issue_reason = "Collection title looks auto-generated or lightly templated"
    elif title_optimization_status == "default_like" and title_optimization_confidence >= 80:
        collection_issue_family = "default_collection_title"
        collection_issue_reason = "Collection title looks close to a default CMS template title"
    elif title_optimization_status in {"default_like", "term_plus_site"}:
        collection_issue_family = "collection_title_needs_review"
        collection_issue_reason = "Collection title looks templated, but confidence is not high enough for a strong default-title claim"
    elif excluded_rows and not selected_rows:
        collection_issue_reason = "Only low-value collection URLs were detected in the saved crawl"

    return {
        "collection_detection_status": collection_detection_status,
        "collection_detection_confidence": collection_detection_confidence,
        "collection_main_content": main_content,
        "collection_main_content_method": main_content_method,
        "collection_main_content_confidence": main_content_confidence,
        "collection_above_raw_text": above_raw_text,
        "collection_below_raw_text": below_raw_text,
        "collection_above_clean_text": above_grid_text,
        "collection_below_clean_text": below_grid_text,
        "collection_best_intro_text": best_intro_text,
        "collection_best_intro_position": best_intro_position,
        "collection_best_intro_confidence": best_intro_confidence,
        "collection_best_intro_source_type": best_intro_source_type,
        "collection_intro_text": intro_text,
        "collection_intro_position": intro_position,
        "collection_intro_status": intro_status,
        "collection_intro_method": intro_method,
        "collection_intro_confidence": intro_confidence,
        "collection_schema_types": schema_types_text,
        "collection_schema_types_method": schema_types_method,
        "collection_schema_types_confidence": schema_types_confidence,
        "collection_product_count": product_count,
        "collection_product_count_method": product_count_method,
        "collection_product_count_confidence": product_count_confidence,
        "collection_title_value": title_value,
        "collection_title_method": title_method,
        "collection_title_confidence": title_confidence,
        "collection_h1_value": h1_value,
        "collection_h1_method": h1_method,
        "collection_h1_confidence": h1_confidence,
        "title_optimization_status": title_optimization_status,
        "title_optimization_confidence": title_optimization_confidence,
        "collection_title_rule_family": platform_family,
        "collection_title_rule_match": title_rule_match,
        "collection_title_rule_confidence": title_optimization_confidence,
        "collection_title_site_name_match": title_site_name_match,
        "collection_issue_family": collection_issue_family,
        "collection_issue_reason": collection_issue_reason,
        "collection_review_url": collection_url,
        "collection_review_score": int(selected_candidate.get("score") or 0),
        "collection_review_status": selected_candidate.get("selection_status") or ("exclude" if excluded_rows and not selected_rows else "unknown"),
    }


def build_screamingfrog_opportunity(payload: dict[str, Any]) -> dict[str, Any]:
    hooks: list[str] = []
    score = 0
    primary_family = ""
    primary_reason = ""
    schema_issue_flags: list[str] = []
    collection_content_issue_flags: list[str] = []
    product_metadata_issue_flags: list[str] = []
    default_title_issue_flags: list[str] = []
    homepage_issue_flags: list[str] = []
    heading_issue_flags = split_pipe_text(payload.get("heading_issue_flags"))
    result_reason = (payload.get("result_reason") or "").strip()
    pages_crawled = int(payload.get("pages_crawled") or 0)
    category_page_count = int(payload.get("category_page_count") or 0)
    product_page_count = int(payload.get("product_page_count") or 0)
    collection_detection_confidence = int(payload.get("collection_detection_confidence") or 0)
    collection_detection_status = str(payload.get("collection_detection_status") or "").strip()
    collection_intro_confidence = int(payload.get("collection_best_intro_confidence") or payload.get("collection_intro_confidence") or 0)
    title_optimization_confidence = int(payload.get("title_optimization_confidence") or 0)
    evidence_grade, evidence_note = classify_crawl_evidence_grade(payload)
    strong_collection_evidence = (
        collection_detection_status == "detected"
        and collection_detection_confidence >= 70
        and category_page_count >= 1
        and pages_crawled >= 3
    )
    strong_intro_evidence = strong_collection_evidence and collection_intro_confidence >= 70
    product_evidence_ok = product_page_count >= 1 and pages_crawled >= 3
    schema_evidence_ok = pages_crawled >= 3
    heading_evidence_ok = int(payload.get("heading_pages_analyzed") or 0) >= 2
    if result_reason == "rate_limited_429":
        homepage_issue_flags.append("recrawl_slower")
        hooks.append("the crawl hit 429 rate limits and should be recrawled at a slower pace before using the findings")
        return {
            "schema_issue_flags": "|".join(schema_issue_flags),
            "collection_content_issue_flags": "|".join(collection_content_issue_flags),
            "product_metadata_issue_flags": "|".join(product_metadata_issue_flags),
            "default_title_issue_flags": "|".join(default_title_issue_flags),
            "homepage_issue_flags": "|".join(homepage_issue_flags),
            "heading_issue_flags": "|".join(dict.fromkeys(heading_issue_flags)),
            "sf_opportunity_score": 0,
            "sf_primary_issue_family": "crawl_failed",
            "sf_primary_issue_reason": "429 Too Many Requests responses were detected during the crawl and this audit should be recrawled at a slower pace",
            "sf_outreach_hooks": " | ".join(hooks[:4]),
            "result_quality": "error",
            "result_reason": result_reason,
        }

    if payload.get("homepage_status_category") in {"redirect", "client_error", "server_error"}:
        homepage_issue_flags.append("homepage_status_issue")
        hooks.append("homepage redirects or does not return a clean 200")
        score += 20
        primary_family = primary_family or "technical_breakage"
        primary_reason = primary_reason or "Homepage is not returning a clean 200"

    if int(payload.get("internal_4xx_count") or 0) > 0 or int(payload.get("internal_5xx_count") or 0) > 0:
        hooks.append(f"key internal URLs are broken ({payload.get('internal_4xx_count', 0)} 4xx / {payload.get('internal_5xx_count', 0)} 5xx)")
        score += 18
        primary_family = primary_family or "technical_breakage"
        primary_reason = primary_reason or "Internal broken URLs were detected"

    intro_status = (payload.get("collection_intro_status") or "").strip()
    collection_issue_family = (payload.get("collection_issue_family") or "").strip()
    if collection_issue_family == "collection_page_not_reviewable":
        if "collection_page_not_reviewable" not in collection_content_issue_flags:
            collection_content_issue_flags.append("collection_page_not_reviewable")
        hooks.append("the crawl did not capture a reliable collection or category page, so collection-specific findings should be treated as unavailable")
        primary_family = primary_family or "crawl_evidence_weak"
        primary_reason = primary_reason or "No reliable collection/category page was captured"
    elif strong_intro_evidence and intro_status == "missing_intro":
        if intro_status not in collection_content_issue_flags:
            collection_content_issue_flags.append(intro_status)
        hooks.append("collection intro copy looks missing or too boilerplate for a commercial category page")
        score += 18
        primary_family = primary_family or "collection_content_gap"
        primary_reason = primary_reason or "Collection intro copy looks weak for a commercial category page"
    elif strong_collection_evidence and intro_status == "boilerplate_only":
        if intro_status not in collection_content_issue_flags:
            collection_content_issue_flags.append(intro_status)
        hooks.append("collection pages appear to rely on boilerplate or UI-heavy copy rather than real merchandising intro text")
        score += 12
        primary_family = primary_family or "collection_content_unclear"
        primary_reason = primary_reason or "Collection content looks boilerplate-heavy rather than category-specific"
    elif strong_collection_evidence and intro_status == "mixed_low_confidence":
        if intro_status not in collection_content_issue_flags:
            collection_content_issue_flags.append(intro_status)
        hooks.append("collection content was found, but the extraction is too mixed to make a confident intro-content claim")
        score += 4
        primary_family = primary_family or "collection_content_unclear"
        primary_reason = primary_reason or "Collection content extraction is too mixed to trust strongly"

    title_optimization_status = (payload.get("title_optimization_status") or "").strip()
    if strong_collection_evidence and title_optimization_status == "default_exact":
        if title_optimization_status not in default_title_issue_flags:
            default_title_issue_flags.append(title_optimization_status)
        hooks.append("collection titles appear close to default CMS or store-name template patterns")
        score += 18
        primary_family = primary_family or "default_collection_title"
        primary_reason = primary_reason or "Collection titles look largely untouched or lightly templated"
    elif strong_collection_evidence and title_optimization_status == "default_like" and title_optimization_confidence >= 80:
        if title_optimization_status not in default_title_issue_flags:
            default_title_issue_flags.append(title_optimization_status)
        hooks.append("collection titles look close to default CMS template patterns")
        score += 10
        primary_family = primary_family or "default_collection_title"
        primary_reason = primary_reason or "Collection titles look close to default template patterns"
    elif strong_collection_evidence and title_optimization_status == "term_plus_site" and title_optimization_confidence >= 70:
        if title_optimization_status not in default_title_issue_flags:
            default_title_issue_flags.append(title_optimization_status)
        hooks.append("collection titles look lightly templated rather than fully customised")
        score += 4
        primary_family = primary_family or "collection_title_needs_review"
        primary_reason = primary_reason or "Collection titles look templated, but not strongly enough to treat as default titles"

    product_metadata_count = len(split_pipe_text(payload.get("title_issue_flags"))) + len(split_pipe_text(payload.get("meta_issue_flags")))
    if product_evidence_ok and product_metadata_count > 0:
        product_metadata_issue_flags.append("product_metadata_gaps")
        hooks.append("product templates show metadata duplication or thin title/meta patterns")
        score += 16
        primary_family = primary_family or "product_metadata_gap"
        primary_reason = primary_reason or "Product metadata patterns are weak"

    if split_pipe_text(payload.get("canonical_issue_flags")):
        hooks.append("important pages show canonical inconsistencies")
        score += 12
        primary_family = primary_family or "indexability_risk"
        primary_reason = primary_reason or "Canonical inconsistencies were found"

    if schema_evidence_ok and int(payload.get("schema_page_count") or 0) == 0:
        schema_issue_flags.append("missing_schema")
        hooks.append("schema appears absent across the audited commercial pages")
        score += 8
        primary_family = primary_family or "schema_gap"
        primary_reason = primary_reason or "Schema is missing across audited pages"

    if heading_evidence_ok and int(payload.get("heading_h1_missing_count") or 0) > 0:
        hooks.append("some audited pages are missing an H1 entirely")
        score += min(10, int(payload.get("heading_h1_missing_count") or 0) * 4)
        primary_family = primary_family or "heading_hygiene"
        primary_reason = primary_reason or "Some pages are missing an H1"

    if heading_evidence_ok and int(payload.get("heading_duplicate_h1_count") or 0) > 0:
        hooks.append("the same H1 appears across multiple audited pages")
        score += min(8, int(payload.get("heading_duplicate_h1_count") or 0) * 3)
        primary_family = primary_family or "heading_hygiene"
        primary_reason = primary_reason or "Heading patterns look duplicated across templates"

    if heading_evidence_ok and int(payload.get("heading_pages_analyzed") or 0) > int(payload.get("heading_pages_with_h2_count") or 0):
        hooks.append("some pages have weak heading depth beyond the H1")
        score += 6
        primary_family = primary_family or "heading_hygiene"
        primary_reason = primary_reason or "Heading depth looks weak on some audited pages"

    if heading_evidence_ok and int(payload.get("heading_generic_heading_count") or 0) > 0:
        hooks.append("some headings still look generic or template-driven")
        score += 6
        primary_family = primary_family or "heading_hygiene"
        primary_reason = primary_reason or "Headings look generic or templated"

    result_quality = "useful"
    result_reason = payload.get("result_reason") or ""
    if payload.get("seed_count", 0) <= 1 or int(payload.get("pages_crawled") or 0) <= 1:
        result_quality = "weak"
        result_reason = result_reason or "bounded crawl only captured the homepage"
    if payload.get("status") == "partial":
        result_quality = "partial"
    if payload.get("status") == "error":
        result_quality = "error"

    if evidence_grade == "D":
        score = min(score, 8)
        if not primary_family or primary_family in {"collection_content_gap", "default_collection_title", "schema_gap", "product_metadata_gap"}:
            primary_family = "crawl_evidence_weak"
            primary_reason = evidence_note
        hooks = [f"the crawl evidence is too thin to use confidently yet ({evidence_note})"]
        if result_quality == "useful":
            result_quality = "weak"
    elif evidence_grade == "C":
        score = min(score, 24)
        if not hooks:
            hooks.append(f"the crawl captured usable but limited evidence ({evidence_note})")

    return {
        "schema_issue_flags": "|".join(schema_issue_flags),
        "collection_content_issue_flags": "|".join(collection_content_issue_flags),
        "product_metadata_issue_flags": "|".join(product_metadata_issue_flags),
        "default_title_issue_flags": "|".join(default_title_issue_flags),
        "homepage_issue_flags": "|".join(homepage_issue_flags),
        "heading_issue_flags": "|".join(dict.fromkeys(heading_issue_flags)),
        "sf_opportunity_score": score,
        "sf_primary_issue_family": primary_family,
        "sf_primary_issue_reason": primary_reason,
        "sf_outreach_hooks": " | ".join(hooks[:4]),
        "result_quality": result_quality,
        "result_reason": result_reason,
    }


def summarize_screamingfrog_exports(
    root_domain: str,
    crawl_mode: str,
    output_dir: Path,
    *,
    resolved_platform_family: str = "generic",
    resolved_config_path: str = "",
    discovery: dict[str, Any] | None = None,
) -> dict[str, Any]:
    internal_rows = read_csv_rows(output_dir / "internal_all.csv")
    title_rows = read_csv_rows(output_dir / "page_titles_all.csv")
    meta_rows = read_csv_rows(output_dir / "meta_description_all.csv")
    h1_rows = read_csv_rows(output_dir / "h1_all.csv")
    canonical_rows = read_csv_rows(output_dir / "canonicals_all.csv")
    directive_rows = read_csv_rows(output_dir / "directives_all.csv")
    structured_rows = read_csv_rows(output_dir / "structured_data_all.csv")
    internal_3xx_rows = read_csv_rows(output_dir / "response_codes_internal_redirection_(3xx).csv")
    internal_4xx_rows = read_csv_rows(output_dir / "response_codes_internal_client_error_(4xx).csv")
    internal_5xx_rows = read_csv_rows(output_dir / "response_codes_internal_server_error_(5xx).csv")

    if not internal_rows:
        payload = {
            "root_domain": root_domain,
            "crawl_mode": crawl_mode,
            "resolved_platform_family": normalize_screamingfrog_platform_family(resolved_platform_family),
            "resolved_config_path": resolved_config_path,
            "requested_homepage_url": (discovery or {}).get("requested_homepage_url", f"https://{root_domain}"),
            "discovered_final_homepage_url": (discovery or {}).get("final_homepage_url", ""),
            "seed_strategy": (discovery or {}).get("seed_strategy", ""),
            "seed_count": (discovery or {}).get("seed_count", 0),
            "sitemap_found": (discovery or {}).get("sitemap_found", 0),
            "sitemap_url": (discovery or {}).get("sitemap_url", ""),
            "sitemap_source": (discovery or {}).get("sitemap_source", ""),
            "checked_at": now_iso(),
            "status": "error",
            "error_message": "Screaming Frog finished without an internal crawl export",
            "pages_crawled": 0,
            "homepage_final_url": "",
            "homepage_status_code": None,
            "homepage_status_category": "",
            "homepage_indexability": "",
            "homepage_title": "",
            "homepage_meta_description": "",
            "homepage_canonical": "",
            "homepage_word_count": 0,
            "redirect_presence": 0,
            "blocked_or_noindex": 0,
            "title_issue_flags": "",
            "meta_issue_flags": "",
            "canonical_issue_flags": "",
            "h1_issue_flags": "",
            "indexable_page_count": 0,
            "internal_3xx_count": 0,
            "internal_4xx_count": 0,
            "internal_5xx_count": 0,
            "schema_page_count": 0,
            "location_page_count": 0,
            "service_page_count": 0,
            "has_internal_errors": 0,
            "export_directory": str(output_dir),
        }
        payload.update(build_heading_intelligence([]))
        payload.update(build_screamingfrog_opportunity(payload))
        return payload

    homepage_row = next((row for row in internal_rows if parse_int_value(row.get("Crawl Depth")) == 0), internal_rows[0])
    homepage_address = parse_csv_address(homepage_row.get("Address"))
    homepage_key = normalized_url_key(homepage_address)

    def matching_row(rows: list[dict[str, str]]) -> dict[str, str] | None:
        for row in rows:
            if normalized_url_key(row.get("Address")) == homepage_key:
                return row
        return rows[0] if rows else None

    title_row = matching_row(title_rows) or {}
    meta_row = matching_row(meta_rows) or {}
    canonical_row = matching_row(canonical_rows) or {}
    directive_row = matching_row(directive_rows) or {}
    homepage_status_code = parse_int_value(homepage_row.get("Status Code"))
    homepage_indexability = (homepage_row.get("Indexability") or canonical_row.get("Indexability") or "").strip()
    homepage_indexability_status = (homepage_row.get("Indexability Status") or directive_row.get("Indexability Status") or "").strip().lower()
    homepage_title = (homepage_row.get("Title 1") or title_row.get("Title 1") or "").strip()
    homepage_meta_description = (homepage_row.get("Meta Description 1") or meta_row.get("Meta Description 1") or "").strip()
    homepage_canonical = (homepage_row.get("Canonical Link Element 1") or canonical_row.get("Canonical Link Element 1") or "").strip()
    homepage_word_count = parse_int_value(homepage_row.get("Word Count"))
    redirect_presence = 1 if internal_3xx_rows else 0
    blocked_or_noindex = 1 if ("noindex" in (directive_row.get("Meta Robots 1") or "").lower() or "blocked by robots" in homepage_indexability_status) else 0
    title_flags = collect_text_issue_flags(title_rows, "Title 1", "Title 1 Length", min_length=30, max_length=60)
    meta_flags = collect_text_issue_flags(meta_rows, "Meta Description 1", "Meta Description 1 Length", min_length=70, max_length=160)
    canonical_flags = collect_canonical_issue_flags(canonical_rows)
    h1_flags = collect_text_issue_flags(h1_rows, "H1-1", "H1-1 Length", min_length=4, max_length=70)
    indexable_page_count = sum(1 for row in internal_rows if (row.get("Indexability") or "").strip().lower() == "indexable")
    schema_page_count = sum(
        1
        for row in structured_rows
        if parse_int_value(row.get("Total Types")) > 0
        or parse_int_value(row.get("Unique Types")) > 0
        or parse_int_value(row.get("Rich Result Features")) > 0
    )
    internal_4xx_count = len(internal_4xx_rows)
    internal_5xx_count = len(internal_5xx_rows)
    rate_limited_rows = detect_screamingfrog_rate_limited_rows(internal_rows) or detect_screamingfrog_rate_limited_rows(internal_4xx_rows)

    payload = {
        "root_domain": root_domain,
        "crawl_mode": crawl_mode,
        "resolved_platform_family": normalize_screamingfrog_platform_family(resolved_platform_family),
        "resolved_config_path": resolved_config_path,
        "requested_homepage_url": (discovery or {}).get("requested_homepage_url", f"https://{root_domain}"),
        "discovered_final_homepage_url": (discovery or {}).get("final_homepage_url", homepage_address),
        "seed_strategy": (discovery or {}).get("seed_strategy", ""),
        "seed_count": (discovery or {}).get("seed_count", 0),
        "sitemap_found": (discovery or {}).get("sitemap_found", 0),
        "sitemap_url": (discovery or {}).get("sitemap_url", ""),
        "sitemap_source": (discovery or {}).get("sitemap_source", ""),
        "result_reason": (discovery or {}).get("result_reason", ""),
        "checked_at": now_iso(),
        "status": "success",
        "error_message": "",
        "pages_crawled": len(internal_rows),
        "homepage_final_url": homepage_address,
        "homepage_status_code": homepage_status_code,
        "homepage_status_category": screamingfrog_homepage_status_category(homepage_status_code),
        "homepage_indexability": homepage_indexability,
        "homepage_title": homepage_title,
        "homepage_meta_description": homepage_meta_description,
        "homepage_canonical": homepage_canonical,
        "homepage_word_count": homepage_word_count,
        "redirect_presence": redirect_presence,
        "blocked_or_noindex": blocked_or_noindex,
        "title_issue_flags": "|".join(title_flags),
        "meta_issue_flags": "|".join(meta_flags),
        "canonical_issue_flags": "|".join(canonical_flags),
        "h1_issue_flags": "|".join(h1_flags),
        "indexable_page_count": indexable_page_count,
        "category_page_count": detect_category_page_count(internal_rows),
        "product_page_count": detect_product_page_count(internal_rows),
        "internal_3xx_count": len(internal_3xx_rows),
        "internal_4xx_count": internal_4xx_count,
        "internal_5xx_count": internal_5xx_count,
        "schema_page_count": schema_page_count,
        "location_page_count": detect_location_page_count(internal_rows),
        "service_page_count": detect_service_page_count(internal_rows),
        "has_internal_errors": 1 if (internal_4xx_count or internal_5xx_count) else 0,
        "export_directory": str(output_dir),
    }
    payload.update(
        build_collection_intelligence(
            payload,
            root_domain=root_domain,
            platform_family=resolved_platform_family,
            internal_rows=internal_rows,
            title_rows=title_rows,
            h1_rows=h1_rows,
            structured_rows=structured_rows,
            directive_rows=directive_rows,
        )
    )
    payload.update(build_heading_intelligence(internal_rows))
    payload.update(build_screamingfrog_opportunity(payload))
    if rate_limited_rows:
        rate_limited_count = len(rate_limited_rows)
        payload["status"] = "error"
        payload["result_quality"] = "error"
        payload["result_reason"] = "rate_limited_429"
        payload["error_message"] = f"Screaming Frog crawl hit 429 Too Many Requests on {rate_limited_count} URL(s); recrawl at a slower pace is required"
        payload.update(build_screamingfrog_opportunity(payload))
    return payload


def run_screamingfrog_crawl(
    root_domain: str,
    crawl_mode: str,
    *,
    resolved_platform_family: str = "generic",
    batch_id: str = "",
    job_id: str = "",
) -> dict[str, Any]:
    launcher = resolve_screamingfrog_launcher()
    normalized_mode = normalize_screamingfrog_crawl_mode(crawl_mode)
    normalized_family = normalize_screamingfrog_platform_family(resolved_platform_family)
    profile, profile_path = load_screamingfrog_profile(normalized_mode, normalized_family)
    profile = apply_cautious_screamingfrog_profile(profile, crawl_mode=normalized_mode, root_domain=root_domain)
    if launcher is None:
        return {
            "root_domain": root_domain,
            "crawl_mode": normalized_mode,
            "resolved_platform_family": normalized_family,
            "resolved_config_path": str(profile_path),
            "checked_at": now_iso(),
            "status": "error",
            "error_message": "Screaming Frog launcher was not found on this Mac",
            "pages_crawled": 0,
            "homepage_final_url": "",
            "homepage_status_code": None,
            "homepage_status_category": "",
            "homepage_indexability": "",
            "homepage_title": "",
            "homepage_meta_description": "",
            "homepage_canonical": "",
            "homepage_word_count": 0,
            "redirect_presence": 0,
            "blocked_or_noindex": 0,
            "title_issue_flags": "",
            "meta_issue_flags": "",
            "canonical_issue_flags": "",
            "h1_issue_flags": "",
            "indexable_page_count": 0,
            "internal_3xx_count": 0,
            "internal_4xx_count": 0,
            "internal_5xx_count": 0,
            "schema_page_count": 0,
            "location_page_count": 0,
            "service_page_count": 0,
            "has_internal_errors": 0,
            "export_directory": "",
        }

    run_stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    output_dir = SCREAMINGFROG_RUNS_ROOT / safe_slug(root_domain) / run_stamp
    output_dir.mkdir(parents=True, exist_ok=True)
    discovery = build_screamingfrog_seed_urls(root_domain, profile)
    seed_urls = discovery["seeds"]
    seed_list_path = output_dir / "seed_urls.txt"
    seed_list_path.write_text("\n".join(seed_urls) + "\n", encoding="utf-8")

    command = [
        str(launcher),
        "--crawl-list",
        str(seed_list_path),
        "--headless",
        "--output-folder",
        str(output_dir),
        "--overwrite",
        "--export-format",
        "csv",
        "--export-tabs",
        ",".join(SCREAMINGFROG_EXPORT_TABS),
    ]
    config_path = resolve_screamingfrog_legacy_app_config(normalized_mode)
    if config_path is not None:
        command.extend(["--config", str(config_path)])

    timeout_seconds = parse_int_value(profile.get("timeout_seconds")) or (600 if normalized_mode == "bounded_audit" else 1200)

    def partial_from_existing_exports(error_message: str, status: str = "partial") -> dict[str, Any] | None:
        if not (output_dir / "internal_all.csv").exists():
            return None
        partial_payload = summarize_screamingfrog_exports(
            root_domain,
            normalized_mode,
            output_dir,
            resolved_platform_family=normalized_family,
            resolved_config_path=str(profile_path),
            discovery=discovery,
        )
        partial_payload["status"] = status
        seed_error = discovery.get("seed_warning", "")
        partial_payload["error_message"] = error_message if not seed_error else f"{error_message} | seed warning: {seed_error}"
        return partial_payload

    try:
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        with SCREAMINGFROG_ACTIVE_PROCESS_LOCK:
            global SCREAMINGFROG_ACTIVE_PROCESS, SCREAMINGFROG_ACTIVE_BATCH_ID, SCREAMINGFROG_ACTIVE_JOB_ID
            SCREAMINGFROG_ACTIVE_PROCESS = process
            SCREAMINGFROG_ACTIVE_BATCH_ID = batch_id
            SCREAMINGFROG_ACTIVE_JOB_ID = job_id
        stdout, stderr = process.communicate(timeout=timeout_seconds)
        result = subprocess.CompletedProcess(
            command,
            process.returncode,
            stdout=stdout,
            stderr=stderr,
        )
    except subprocess.TimeoutExpired:
        with SCREAMINGFROG_ACTIVE_PROCESS_LOCK:
            if SCREAMINGFROG_ACTIVE_PROCESS is not None:
                try:
                    SCREAMINGFROG_ACTIVE_PROCESS.kill()
                except Exception:
                    pass
        partial_payload = partial_from_existing_exports(f"Screaming Frog timed out after {timeout_seconds} seconds")
        if partial_payload is not None:
            return partial_payload
        return {
            "root_domain": root_domain,
            "crawl_mode": normalized_mode,
            "resolved_platform_family": normalized_family,
            "resolved_config_path": str(profile_path),
            "checked_at": now_iso(),
            "status": "error",
            "error_message": (
                f"Screaming Frog timed out after {timeout_seconds} seconds"
                if not discovery.get("seed_warning")
                else f"Screaming Frog timed out after {timeout_seconds} seconds | seed warning: {discovery.get('seed_warning')}"
            ),
            "pages_crawled": 0,
            "homepage_final_url": "",
            "homepage_status_code": None,
            "homepage_status_category": "",
            "homepage_indexability": "",
            "homepage_title": "",
            "homepage_meta_description": "",
            "homepage_canonical": "",
            "homepage_word_count": 0,
            "redirect_presence": 0,
            "blocked_or_noindex": 0,
            "title_issue_flags": "",
            "meta_issue_flags": "",
            "canonical_issue_flags": "",
            "h1_issue_flags": "",
            "indexable_page_count": 0,
            "internal_3xx_count": 0,
            "internal_4xx_count": 0,
            "internal_5xx_count": 0,
            "schema_page_count": 0,
            "location_page_count": 0,
            "service_page_count": 0,
            "has_internal_errors": 0,
            "export_directory": str(output_dir),
        }
    finally:
        with SCREAMINGFROG_ACTIVE_PROCESS_LOCK:
            SCREAMINGFROG_ACTIVE_PROCESS = None
            SCREAMINGFROG_ACTIVE_BATCH_ID = ""
            SCREAMINGFROG_ACTIVE_JOB_ID = ""

    if result.returncode != 0:
        if batch_id and batch_id in SCREAMINGFROG_STOP_REQUESTED_BATCHES:
            return {
                "root_domain": root_domain,
                "crawl_mode": normalized_mode,
                "resolved_platform_family": normalized_family,
                "resolved_config_path": str(profile_path),
                "checked_at": now_iso(),
                "status": "error",
                "error_message": "Stopped by user from the app",
                "result_quality": "error",
                "result_reason": "user_stopped",
                "pages_crawled": 0,
                "homepage_final_url": "",
                "homepage_status_code": None,
                "homepage_status_category": "",
                "homepage_indexability": "",
                "homepage_title": "",
                "homepage_meta_description": "",
                "homepage_canonical": "",
                "homepage_word_count": 0,
                "redirect_presence": 0,
                "blocked_or_noindex": 0,
                "title_issue_flags": "",
                "meta_issue_flags": "",
                "canonical_issue_flags": "",
                "h1_issue_flags": "",
                "indexable_page_count": 0,
                "internal_3xx_count": 0,
                "internal_4xx_count": 0,
                "internal_5xx_count": 0,
                "schema_page_count": 0,
                "location_page_count": 0,
                "service_page_count": 0,
                "has_internal_errors": 0,
                "export_directory": str(output_dir),
            }
        message = (result.stderr or result.stdout or "").strip().splitlines()
        error_message = message[-1] if message else f"Screaming Frog exited with code {result.returncode}"
        partial_payload = partial_from_existing_exports(error_message)
        if partial_payload is not None:
            return partial_payload
        return {
            "root_domain": root_domain,
            "crawl_mode": normalized_mode,
            "resolved_platform_family": normalized_family,
            "resolved_config_path": str(profile_path),
            "checked_at": now_iso(),
            "status": "error",
            "error_message": error_message if not discovery.get("seed_warning") else f"{error_message} | seed warning: {discovery.get('seed_warning')}",
            "pages_crawled": 0,
            "homepage_final_url": "",
            "homepage_status_code": None,
            "homepage_status_category": "",
            "homepage_indexability": "",
            "homepage_title": "",
            "homepage_meta_description": "",
            "homepage_canonical": "",
            "homepage_word_count": 0,
            "redirect_presence": 0,
            "blocked_or_noindex": 0,
            "title_issue_flags": "",
            "meta_issue_flags": "",
            "canonical_issue_flags": "",
            "h1_issue_flags": "",
            "indexable_page_count": 0,
            "internal_3xx_count": 0,
            "internal_4xx_count": 0,
            "internal_5xx_count": 0,
            "schema_page_count": 0,
            "location_page_count": 0,
            "service_page_count": 0,
            "has_internal_errors": 0,
            "export_directory": str(output_dir),
        }

    payload = summarize_screamingfrog_exports(
        root_domain,
        normalized_mode,
        output_dir,
        resolved_platform_family=normalized_family,
        resolved_config_path=str(profile_path),
        discovery=discovery,
    )
    if discovery.get("seed_warning"):
        payload["status"] = "partial"
        payload["error_message"] = f"Seed discovery warning: {discovery.get('seed_warning')}"
    elif payload.get("result_quality") == "weak":
        payload["status"] = "partial"
        payload["error_message"] = payload.get("result_reason") or "Bounded crawl produced a weak result"
    return payload


def selected_tray_screamingfrog_candidates(connection: sqlite3.Connection, crawl_mode: str) -> list[dict[str, Any]]:
    domains = get_selected_tray_domains(connection)
    if not domains:
        return []
    placeholders = ", ".join("?" for _ in domains)
    rows = connection.execute(
        f"""
        select
            leads.root_domain,
            leads.company,
            coalesce(leads.current_platforms, '') as current_platforms
        from leads
        where leads.root_domain in ({placeholders})
        order by leads.root_domain asc
        """,
        domains,
    ).fetchall()
    existing = {
        row["root_domain"]
        for row in connection.execute("select root_domain from state.screamingfrog_audit_snapshots").fetchall()
    }
    launcher = resolve_screamingfrog_launcher()
    return [
        {
            "root_domain": row["root_domain"],
            "company": row["company"],
            "resolved_platform_family": resolve_screamingfrog_platform_family(split_pipe(row["current_platforms"])),
            "eligible": launcher is not None,
            "eligibility_reason": "" if launcher is not None else "Screaming Frog launcher was not found on this Mac",
            "already_audited": row["root_domain"] in existing,
        }
        for row in rows
    ]


def summarize_screamingfrog_candidates(candidates: list[dict[str, Any]], *, skip_existing: bool) -> dict[str, Any]:
    selected_count = len(candidates)
    eligible = [item for item in candidates if item["eligible"]]
    already_audited = [item for item in eligible if item["already_audited"]]
    to_run = [item for item in eligible if not skip_existing or not item["already_audited"]]
    excluded = [{"root_domain": item["root_domain"], "reason": item["eligibility_reason"]} for item in candidates if not item["eligible"]]
    breakdown_counter = Counter(item["resolved_platform_family"] for item in eligible)
    return {
        "selectedCount": selected_count,
        "eligibleCount": len(eligible),
        "alreadyAuditedCount": len(already_audited),
        "toRunCount": len(to_run),
        "estimatedRuns": len(to_run),
        "resolvedConfigBreakdown": [
            {"platformFamily": family, "label": SCREAMINGFROG_PLATFORM_LABELS.get(family, family.replace("_", " ").title()), "count": count}
            for family, count in sorted(breakdown_counter.items())
        ],
        "excluded": excluded,
    }


def enqueue_screamingfrog_jobs(connection: sqlite3.Connection, *, crawl_mode: str, refresh_existing: bool) -> dict[str, Any]:
    normalized_mode = normalize_screamingfrog_crawl_mode(crawl_mode)
    candidates = selected_tray_screamingfrog_candidates(connection, normalized_mode)
    summary = summarize_screamingfrog_candidates(candidates, skip_existing=not refresh_existing)
    to_run = [item for item in candidates if item["eligible"] and (refresh_existing or not item["already_audited"])]
    if not to_run:
        active = fetch_active_screamingfrog_batch(connection, tray_domains=[item["root_domain"] for item in candidates])
        return {"crawlMode": normalized_mode, "summary": summary, "results": [], "jobBatch": active}

    batch_id = str(uuid.uuid4())
    created_at = now_iso()
    tray_domains = [item["root_domain"] for item in to_run]
    if tray_domains:
        placeholders = ", ".join("?" for _ in tray_domains)
        connection.execute(
            f"""
            update state.screamingfrog_jobs
            set status = 'error',
                message = 'Superseded by newer crawl batch',
                result_quality = 'error',
                result_reason = 'superseded_by_newer_batch',
                completed_at = ?,
                updated_at = ?
            where root_domain in ({placeholders})
              and status = 'queued'
            """,
            (created_at, created_at, *tray_domains),
        )
    for item in to_run:
        connection.execute(
            """
            insert into state.screamingfrog_jobs (
                id,
                batch_id,
                root_domain,
                crawl_mode,
                resolved_platform_family,
                status,
                created_at,
                updated_at
            ) values (?, ?, ?, ?, ?, 'queued', ?, ?)
            """,
            (
                str(uuid.uuid4()),
                batch_id,
                item["root_domain"],
                normalized_mode,
                item["resolved_platform_family"],
                created_at,
                created_at,
            ),
        )
    connection.commit()
    ensure_screamingfrog_worker()
    active = fetch_screamingfrog_batch(connection, batch_id)
    return {"crawlMode": normalized_mode, "summary": summary, "results": [], "jobBatch": active}


def update_screamingfrog_job(job_id: str, **fields: Any) -> None:
    if not fields:
        return
    fields["updated_at"] = now_iso()
    assignments = ", ".join(f"{key} = :{key}" for key in fields)
    with get_state_connection() as connection:
        payload = {**fields, "id": job_id}
        connection.execute(f"update screamingfrog_jobs set {assignments} where id = :id", payload)
        connection.commit()


def fetch_screamingfrog_batch(connection: sqlite3.Connection, batch_id: str) -> dict[str, Any] | None:
    rows = connection.execute(
        """
        select *
        from state.screamingfrog_jobs
        where batch_id = ?
        order by created_at asc, root_domain asc
        """,
        (batch_id,),
    ).fetchall()
    if not rows:
        return None
    status_counter = Counter(row["status"] for row in rows)
    return {
        "batchId": batch_id,
        "isActive": any(status in {"queued", "discovering", "running"} for status in status_counter),
        "counts": dict(status_counter),
        "items": [dict(row) for row in rows],
    }


def fetch_active_screamingfrog_batch(connection: sqlite3.Connection, *, tray_domains: list[str]) -> dict[str, Any] | None:
    if not tray_domains:
        return None
    placeholders = ", ".join("?" for _ in tray_domains)
    row = connection.execute(
        f"""
        select batch_id
        from state.screamingfrog_jobs
        where root_domain in ({placeholders})
          and status in ('queued', 'discovering', 'running')
        order by created_at desc
        limit 1
        """,
        tray_domains,
    ).fetchone()
    if not row:
        return None
    return fetch_screamingfrog_batch(connection, row["batch_id"])


def stop_screamingfrog_batch(batch_id: str) -> dict[str, Any] | None:
    SCREAMINGFROG_STOP_REQUESTED_BATCHES.add(batch_id)
    with get_state_connection() as connection:
        connection.execute(
            """
            update screamingfrog_jobs
            set status = 'error',
                message = case
                    when status = 'queued' then 'Stopped by user before crawl started'
                    when status = 'discovering' then 'Stopped by user during seed discovery'
                    when status = 'running' then 'Stopped by user during crawl'
                    else message
                end,
                result_quality = case
                    when status in ('queued', 'discovering', 'running') then 'error'
                    else result_quality
                end,
                result_reason = case
                    when status in ('queued', 'discovering', 'running') then 'user_stopped'
                    else result_reason
                end,
                completed_at = case
                    when status in ('queued', 'discovering', 'running') then ?
                    else completed_at
                end,
                updated_at = ?
            where batch_id = ?
              and status in ('queued', 'discovering', 'running')
            """,
            (now_iso(), now_iso(), batch_id),
        )
        connection.commit()
        rows = connection.execute(
            """
            select *
            from screamingfrog_jobs
            where batch_id = ?
            order by created_at asc, root_domain asc
            """,
            (batch_id,),
        ).fetchall()
        if rows:
            status_counter = Counter(row["status"] for row in rows)
            batch = {
                "batchId": batch_id,
                "isActive": any(status in {"queued", "discovering", "running"} for status in status_counter),
                "counts": dict(status_counter),
                "items": [dict(row) for row in rows],
            }
        else:
            batch = None
    with SCREAMINGFROG_ACTIVE_PROCESS_LOCK:
        if SCREAMINGFROG_ACTIVE_PROCESS is not None and SCREAMINGFROG_ACTIVE_BATCH_ID == batch_id:
            try:
                SCREAMINGFROG_ACTIVE_PROCESS.terminate()
            except Exception:
                pass
    return batch


def resolve_screamingfrog_export_directory(connection: sqlite3.Connection, root_domain: str) -> Path:
    row = connection.execute(
        """
        select export_directory
        from state.screamingfrog_audit_snapshots
        where root_domain = ?
        """,
        (root_domain,),
    ).fetchone()
    if row is None or not str(row["export_directory"] or "").strip():
        raise HTTPException(status_code=404, detail="No Screaming Frog audit exports saved for this domain")
    export_directory = Path(str(row["export_directory"])).resolve()
    if not export_directory.exists() or not export_directory.is_dir():
        raise HTTPException(status_code=404, detail="Saved Screaming Frog export directory is missing")
    return export_directory


def build_screamingfrog_audit_file_entries(export_directory: Path, root_domain: str) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for file_path in sorted(export_directory.iterdir(), key=lambda item: item.name.lower()):
        if not file_path.is_file():
            continue
        stat = file_path.stat()
        entries.append(
            {
                "name": file_path.name,
                "size": stat.st_size,
                "modified_at": datetime.fromtimestamp(stat.st_mtime, UTC).isoformat(),
                "download_url": f"/api/screamingfrog/audit/file?{urlencode({'root_domain': root_domain, 'name': file_path.name})}",
            }
        )
    return entries


def load_screamingfrog_csv(export_directory: Path, filename: str) -> list[dict[str, str]]:
    file_path = export_directory / filename
    if not file_path.exists() or not file_path.is_file():
        return []
    with file_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return [{str(key or "").strip(): str(value or "").strip() for key, value in row.items()} for row in reader]


def load_screamingfrog_seed_urls(export_directory: Path) -> list[str]:
    file_path = export_directory / "seed_urls.txt"
    if not file_path.exists() or not file_path.is_file():
        return []
    return [line.strip() for line in file_path.read_text(encoding="utf-8").splitlines() if line.strip()]


def screamingfrog_detect_title_issue(row: Mapping[str, str]) -> str:
    title = row.get("Title 1", "")
    occurrences = parse_int_value(row.get("Occurrences"))
    length = parse_int_value(row.get("Title 1 Length"))
    if not title:
        return "Missing title"
    if occurrences > 1:
        return "Duplicate title"
    if length and length > 65:
        return "Title too long"
    if length and length < 30:
        return "Title too short"
    return "Review"


def screamingfrog_detect_meta_issue(row: Mapping[str, str]) -> str:
    meta = row.get("Meta Description 1", "")
    occurrences = parse_int_value(row.get("Occurrences"))
    length = parse_int_value(row.get("Meta Description 1 Length"))
    if not meta:
        return "Missing meta description"
    if occurrences > 1:
        return "Duplicate meta description"
    if length and length > 160:
        return "Meta too long"
    if length and length < 70:
        return "Meta too short"
    return "Review"


def screamingfrog_detect_canonical_issue(row: Mapping[str, str]) -> str:
    canonical = row.get("Canonical Link Element 1", "")
    indexability = row.get("Indexability", "")
    address = row.get("Address", "")
    if not canonical:
        return "Missing canonical"
    if indexability and indexability.lower() != "indexable":
        return "Non-indexable canonical"
    if canonical.rstrip("/") != address.rstrip("/"):
        return "Canonical differs"
    return "Review"


def screamingfrog_detect_h1_issue(row: Mapping[str, str]) -> str:
    h1 = row.get("H1-1", "")
    occurrences = parse_int_value(row.get("Occurrences"))
    length = parse_int_value(row.get("H1-1 Length"))
    if not h1:
        return "Missing H1"
    if occurrences > 1:
        return "Duplicate H1"
    if length and length > 70:
        return "H1 too long"
    return "Review"


def screamingfrog_detect_schema_issue(row: Mapping[str, str]) -> str:
    rich_features = parse_int_value(row.get("Rich Result Features"))
    total_types = parse_int_value(row.get("Total Types"))
    rich_errors = parse_int_value(row.get("Rich Result Errors"))
    rich_warnings = parse_int_value(row.get("Rich Result Warnings"))
    if rich_errors:
        return "Structured data errors"
    if rich_warnings:
        return "Structured data warnings"
    if rich_features <= 0 and total_types <= 0:
        return "No structured data detected"
    return "Review"


def screamingfrog_detect_directive_issue(row: Mapping[str, str]) -> str:
    meta_robots = row.get("Meta Robots 1", "")
    x_robots = row.get("X-Robots-Tag 1", "")
    indexability = row.get("Indexability", "")
    if meta_robots or x_robots:
        return "Robots directive present"
    if indexability and indexability.lower() != "indexable":
        return "Not indexable"
    return "Review"


def build_screamingfrog_collection_tab(
    export_directory: Path,
    *,
    root_domain: str,
    platform_family: str,
) -> dict[str, Any]:
    internal_rows = load_screamingfrog_csv(export_directory, "internal_all.csv")
    title_rows = load_screamingfrog_csv(export_directory, "page_titles_all.csv")
    h1_rows = load_screamingfrog_csv(export_directory, "h1_all.csv")
    meta_rows = load_screamingfrog_csv(export_directory, "meta_description_all.csv")
    schema_rows = load_screamingfrog_csv(export_directory, "structured_data_all.csv")

    collection_rows = [
        row for row in internal_rows
        if classify_collection_page(csv_row_value(row, "Address"), platform_family) == "collection"
    ][:12]

    def keyed(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
        mapping: dict[str, dict[str, str]] = {}
        for row in rows:
            key = normalized_url_key(csv_row_value(row, "Address"))
            if key and key not in mapping:
                mapping[key] = row
        return mapping

    title_map = keyed(title_rows)
    h1_map = keyed(h1_rows)
    meta_map = keyed(meta_rows)
    schema_map = keyed(schema_rows)
    selected_rows, excluded_rows = select_reviewable_collection_rows(
        internal_rows,
        platform_family=platform_family,
        title_rows=title_rows,
        h1_rows=h1_rows,
        max_rows=12,
    )

    rows: list[dict[str, str]] = []
    for candidate in selected_rows:
        row = candidate["row"]
        address = csv_row_value(row, "Address")
        key = normalized_url_key(address)
        title_row = candidate.get("title_row") or title_map.get(key, {})
        h1_row = candidate.get("h1_row") or h1_map.get(key, {})
        meta_row = meta_map.get(key, {})
        schema_row = schema_map.get(key, {})
        extracted = candidate.get("extracted_content") or (extract_collection_page_content(address) if address else {
            "above_raw_text": "",
            "below_raw_text": "",
            "above_text": "",
            "below_text": "",
            "content_presence": "unknown",
            "error": "",
        })

        title = csv_row_value(title_row, "Title 1") or csv_row_value(row, "Title 1")
        h1 = csv_row_value(h1_row, "H1-1") or csv_row_value(row, "H1-1")
        meta_description = csv_row_value(meta_row, "Meta Description 1") or csv_row_value(row, "Meta Description 1")
        word_count = parse_int_value(csv_row_value(row, "Word Count"))
        above_raw_text = normalize_collection_text(extracted.get("above_raw_text") or "")
        below_raw_text = normalize_collection_text(extracted.get("below_raw_text") or "")
        above_text = normalize_collection_text(extracted.get("above_text") or "")
        below_text = normalize_collection_text(extracted.get("below_text") or "")
        if above_text:
            intro_status = "strong_intro"
        elif below_text:
            intro_status = "below_grid_copy"
        else:
            intro_status = "missing_intro"
        title_optimization = classify_title_optimization_status(
            title,
            h1,
            root_domain,
            platform_family,
            load_collection_intelligence_rule_pack(platform_family).get("title_rules", {}),
        )
        schema_issue = screamingfrog_detect_schema_issue(schema_row) if schema_row else "No structured data detected"

        rows.append(
            {
                "URL": address,
                "Review status": humanize_token(candidate.get("selection_status") or "unknown"),
                "Review score": str(candidate.get("score") or 0),
                "Title": title,
                "Title issue": screamingfrog_detect_title_issue(title_row or row),
                "H1": h1,
                "H1 issue": screamingfrog_detect_h1_issue(h1_row or row),
                "Meta description": meta_description or "—",
                "Content signal": humanize_token(intro_status or "unknown"),
                "Above-grid cleaned": above_text or "—",
                "Below-grid cleaned": below_text or "—",
                "Above-grid raw": above_raw_text or "—",
                "Below-grid raw": below_raw_text or "—",
                "Word count": str(word_count or 0),
                "Title optimisation": humanize_token(title_optimization or "unknown"),
                "Schema": humanize_token(schema_issue or "unknown"),
            }
        )

    return {
        "id": "collections",
        "label": "Collections",
        "count": len(rows),
        "empty": "No useful collection or category pages were captured in the saved audit.",
        "columns": ["URL", "Review status", "Review score", "Title", "Title issue", "H1", "H1 issue", "Meta description", "Content signal", "Above-grid cleaned", "Below-grid cleaned", "Above-grid raw", "Below-grid raw", "Word count", "Title optimisation", "Schema"],
        "rows": rows,
    }


def build_screamingfrog_audit_tabs(export_directory: Path) -> list[dict[str, Any]]:
    titles = load_screamingfrog_csv(export_directory, "page_titles_all.csv")
    meta = load_screamingfrog_csv(export_directory, "meta_description_all.csv")
    canonicals = load_screamingfrog_csv(export_directory, "canonicals_all.csv")
    h1_rows = load_screamingfrog_csv(export_directory, "h1_all.csv")
    schema_rows = load_screamingfrog_csv(export_directory, "structured_data_all.csv")
    directives = load_screamingfrog_csv(export_directory, "directives_all.csv")
    internal_rows = load_screamingfrog_csv(export_directory, "internal_all.csv")
    errors_3xx = load_screamingfrog_csv(export_directory, "response_codes_internal_redirection_(3xx).csv")
    errors_4xx = load_screamingfrog_csv(export_directory, "response_codes_internal_client_error_(4xx).csv")
    errors_5xx = load_screamingfrog_csv(export_directory, "response_codes_internal_server_error_(5xx).csv")
    seeds = load_screamingfrog_seed_urls(export_directory)
    platform_family = "generic"
    try:
        with get_state_connection() as connection:
            row = connection.execute(
                "select resolved_platform_family from screamingfrog_audit_snapshots where export_directory = ? limit 1",
                (str(export_directory),),
            ).fetchone()
            if row and str(row["resolved_platform_family"] or "").strip():
                platform_family = str(row["resolved_platform_family"]).strip()
    except sqlite3.Error:
        platform_family = "generic"

    tabs: list[dict[str, Any]] = []
    tabs.append(build_screamingfrog_collection_tab(export_directory, root_domain=export_directory.parent.name, platform_family=platform_family))
    heading_intelligence = build_heading_intelligence(internal_rows)
    tabs.append(
        {
            "id": "titles",
            "label": "Titles",
            "count": len(titles),
            "empty": "No title rows found in the saved audit.",
            "columns": ["URL", "Issue", "Title", "Length", "Indexability"],
            "rows": [
                {
                    "URL": row.get("Address", ""),
                    "Issue": screamingfrog_detect_title_issue(row),
                    "Title": row.get("Title 1", ""),
                    "Length": row.get("Title 1 Length", ""),
                    "Indexability": row.get("Indexability", ""),
                }
                for row in titles
            ],
        }
    )
    tabs.append(
        {
            "id": "meta",
            "label": "Meta",
            "count": len(meta),
            "empty": "No meta description rows found in the saved audit.",
            "columns": ["URL", "Issue", "Meta description", "Length", "Indexability"],
            "rows": [
                {
                    "URL": row.get("Address", ""),
                    "Issue": screamingfrog_detect_meta_issue(row),
                    "Meta description": row.get("Meta Description 1", ""),
                    "Length": row.get("Meta Description 1 Length", ""),
                    "Indexability": row.get("Indexability", ""),
                }
                for row in meta
            ],
        }
    )
    tabs.append(
        {
            "id": "canonicals",
            "label": "Canonicals",
            "count": len(canonicals),
            "empty": "No canonical rows found in the saved audit.",
            "columns": ["URL", "Issue", "Canonical", "Indexability", "Next/Prev"],
            "rows": [
                {
                    "URL": row.get("Address", ""),
                    "Issue": screamingfrog_detect_canonical_issue(row),
                    "Canonical": row.get("Canonical Link Element 1", ""),
                    "Indexability": row.get("Indexability", ""),
                    "Next/Prev": " | ".join(
                        value for value in [row.get('rel="next" 1', ""), row.get('rel="prev" 1', "")] if value
                    ),
                }
                for row in canonicals
            ],
        }
    )
    tabs.append(
        {
            "id": "h1",
            "label": "H1",
            "count": len(h1_rows),
            "empty": "No H1 rows found in the saved audit.",
            "columns": ["URL", "Issue", "H1", "Length", "Indexability"],
            "rows": [
                {
                    "URL": row.get("Address", ""),
                    "Issue": screamingfrog_detect_h1_issue(row),
                    "H1": row.get("H1-1", ""),
                    "Length": row.get("H1-1 Length", ""),
                    "Indexability": row.get("Indexability", ""),
                }
                for row in h1_rows
            ],
        }
    )
    tabs.append(
        {
            "id": "headings",
            "label": "Headings",
            "count": len(heading_intelligence["page_rows"]),
            "empty": "No heading rows were extracted from the saved audit pages.",
            "columns": ["URL", "Issue", "H1", "H2", "H3", "H4-H6", "All headings"],
            "rows": heading_intelligence["page_rows"],
        }
    )
    tabs.append(
        {
            "id": "schema",
            "label": "Schema",
            "count": len(schema_rows),
            "empty": "No structured data rows found in the saved audit.",
            "columns": ["URL", "Issue", "Types", "Rich results", "Errors / warnings"],
            "rows": [
                {
                    "URL": row.get("Address", ""),
                    "Issue": screamingfrog_detect_schema_issue(row),
                    "Types": row.get("Total Types", ""),
                    "Rich results": row.get("Rich Result Features", ""),
                    "Errors / warnings": f"{row.get('Rich Result Errors', '0')} / {row.get('Rich Result Warnings', '0')}",
                }
                for row in schema_rows
            ],
        }
    )
    tabs.append(
        {
            "id": "directives",
            "label": "Directives",
            "count": len(directives),
            "empty": "No directives rows found in the saved audit.",
            "columns": ["URL", "Issue", "Meta robots", "X-Robots", "Indexability", "Canonical"],
            "rows": [
                {
                    "URL": row.get("Address", ""),
                    "Issue": screamingfrog_detect_directive_issue(row),
                    "Meta robots": row.get("Meta Robots 1", ""),
                    "X-Robots": row.get("X-Robots-Tag 1", ""),
                    "Indexability": row.get("Indexability", ""),
                    "Canonical": row.get("Canonical Link Element 1", ""),
                }
                for row in directives
            ],
        }
    )
    tabs.append(
        {
            "id": "internal",
            "label": "Internal URLs",
            "count": len(internal_rows),
            "empty": "No internal URL rows found in the saved audit.",
            "columns": ["URL", "Status", "Indexability", "Title", "H1", "Word count", "Depth", "Inlinks"],
            "rows": [
                {
                    "URL": row.get("Address", ""),
                    "Status": f"{row.get('Status Code', '')} {row.get('Status', '')}".strip(),
                    "Indexability": row.get("Indexability", ""),
                    "Title": row.get("Title 1", ""),
                    "H1": row.get("H1-1", ""),
                    "Word count": row.get("Word Count", ""),
                    "Depth": row.get("Crawl Depth", ""),
                    "Inlinks": row.get("Inlinks", ""),
                }
                for row in internal_rows
            ],
        }
    )

    error_rows: list[dict[str, str]] = []
    for label, rows in [("3xx redirect", errors_3xx), ("4xx error", errors_4xx), ("5xx error", errors_5xx)]:
        for row in rows:
            error_rows.append(
                {
                    "URL": row.get("Address", ""),
                    "Issue": label,
                    "Status": f"{row.get('Status Code', '')} {row.get('Status', '')}".strip(),
                    "Indexability": row.get("Indexability", ""),
                    "Redirect URL": row.get("Redirect URL", ""),
                    "Inlinks": row.get("Inlinks", ""),
                }
            )
    tabs.append(
        {
            "id": "errors",
            "label": "Errors",
            "count": len(error_rows),
            "empty": "No internal redirects or errors were exported in this crawl.",
            "columns": ["URL", "Issue", "Status", "Indexability", "Redirect URL", "Inlinks"],
            "rows": error_rows,
        }
    )
    tabs.append(
        {
            "id": "seeds",
            "label": "Seeds",
            "count": len(seeds),
            "empty": "No seed URLs were saved for this crawl.",
            "columns": ["Seed URL"],
            "rows": [{"Seed URL": item} for item in seeds],
        }
    )
    return tabs


def ensure_screamingfrog_worker() -> None:
    global SCREAMINGFROG_WORKER_RUNNING, SCREAMINGFROG_WORKER_THREAD
    with SCREAMINGFROG_WORKER_LOCK:
        if SCREAMINGFROG_WORKER_THREAD is not None and SCREAMINGFROG_WORKER_THREAD.is_alive():
            SCREAMINGFROG_WORKER_RUNNING = True
            print("[sf-worker] existing worker thread is alive", flush=True)
            return
        SCREAMINGFROG_WORKER_RUNNING = True
        print("[sf-worker] starting worker thread", flush=True)

    def worker() -> None:
        global SCREAMINGFROG_WORKER_RUNNING, SCREAMINGFROG_WORKER_THREAD
        try:
            print("[sf-worker] worker loop entered", flush=True)
            while True:
                try:
                    with get_state_connection() as connection:
                        job = connection.execute(
                            """
                        select *
                        from screamingfrog_jobs
                        where status = 'queued'
                        order by created_at desc
                        limit 1
                        """
                    ).fetchone()
                except Exception as exc:
                    print(f"[sf-worker] failed reading queued jobs: {exc}", flush=True)
                    raise
                if not job:
                    print("[sf-worker] no queued jobs found, exiting", flush=True)
                    break
                print(f"[sf-worker] picked job {job['id']} for {job['root_domain']}", flush=True)
                process_screamingfrog_job(dict(job))
        finally:
            with SCREAMINGFROG_WORKER_LOCK:
                SCREAMINGFROG_WORKER_RUNNING = False
                SCREAMINGFROG_WORKER_THREAD = None
            print("[sf-worker] worker loop exited", flush=True)

    thread = threading.Thread(target=worker, daemon=True, name="screamingfrog-worker")
    with SCREAMINGFROG_WORKER_LOCK:
        SCREAMINGFROG_WORKER_THREAD = thread
    thread.start()


def process_screamingfrog_job(job: dict[str, Any]) -> None:
    job_id = job["id"]
    batch_id = job["batch_id"]
    if batch_id in SCREAMINGFROG_STOP_REQUESTED_BATCHES:
        update_screamingfrog_job(
            job_id,
            status="error",
            completed_at=now_iso(),
            message="Stopped by user before crawl started",
            result_quality="error",
            result_reason="user_stopped",
        )
        return
    update_screamingfrog_job(job_id, status="discovering", started_at=now_iso(), message="Discovering redirect and sitemap seeds")
    if batch_id in SCREAMINGFROG_STOP_REQUESTED_BATCHES:
        update_screamingfrog_job(
            job_id,
            status="error",
            completed_at=now_iso(),
            message="Stopped by user during seed discovery",
            result_quality="error",
            result_reason="user_stopped",
        )
        return
    update_screamingfrog_job(job_id, status="running", message="Running Screaming Frog locally on this Mac")
    try:
        payload = run_screamingfrog_crawl(
            job["root_domain"],
            job["crawl_mode"],
            resolved_platform_family=job["resolved_platform_family"],
            batch_id=batch_id,
            job_id=job_id,
        )
    except Exception as exc:
        update_screamingfrog_job(
            job_id,
            status="error",
            completed_at=now_iso(),
            message=str(exc) or "Screaming Frog crawl crashed before results were saved",
            result_quality="error",
            result_reason="worker_exception",
        )
        return
    update_screamingfrog_job(
        job_id,
        status=payload["status"],
        completed_at=now_iso(),
        message=payload.get("sf_primary_issue_reason") or payload.get("result_reason") or payload.get("error_message") or "",
        requested_homepage_url=payload.get("requested_homepage_url", ""),
        final_homepage_url=payload.get("discovered_final_homepage_url", ""),
        redirect_detected=1 if normalized_url_key(payload.get("requested_homepage_url")) != normalized_url_key(payload.get("discovered_final_homepage_url")) else 0,
        sitemap_found=payload.get("sitemap_found", 0),
        sitemap_url=payload.get("sitemap_url", ""),
        sitemap_source=payload.get("sitemap_source", ""),
        seed_strategy=payload.get("seed_strategy", ""),
        seed_count=payload.get("seed_count", 0),
        result_quality=payload.get("result_quality", ""),
        result_reason=payload.get("result_reason", ""),
    )
    if batch_id in SCREAMINGFROG_STOP_REQUESTED_BATCHES:
        with SCREAMINGFROG_WORKER_LOCK:
            SCREAMINGFROG_STOP_REQUESTED_BATCHES.discard(batch_id)
    with get_connection() as connection:
        persist_screamingfrog_snapshot(connection, payload)
        connection.commit()
    time.sleep(4)


def persist_screamingfrog_snapshot(connection: sqlite3.Connection, payload: dict[str, Any]) -> None:
    payload = {
        "heading_issue_flags": "",
        "heading_outline_score": 0,
        "heading_outline_summary": "",
        "heading_pages_analyzed": 0,
        "heading_h1_missing_count": 0,
        "heading_multiple_h1_count": 0,
        "heading_duplicate_h1_count": 0,
        "heading_pages_with_h2_count": 0,
        "heading_generic_heading_count": 0,
        "heading_repeated_heading_count": 0,
        **payload,
    }
    connection.execute(
        """
        insert into state.screamingfrog_audit_snapshots (
            root_domain,
            crawl_mode,
            resolved_platform_family,
            resolved_config_path,
            requested_homepage_url,
            discovered_final_homepage_url,
            seed_strategy,
            seed_count,
            sitemap_found,
            sitemap_url,
            sitemap_source,
            result_quality,
            result_reason,
            checked_at,
            status,
            error_message,
            pages_crawled,
            homepage_final_url,
            homepage_status_code,
            homepage_status_category,
            homepage_indexability,
            homepage_title,
            homepage_meta_description,
            homepage_canonical,
            homepage_word_count,
            redirect_presence,
            blocked_or_noindex,
            title_issue_flags,
            meta_issue_flags,
            canonical_issue_flags,
            h1_issue_flags,
            indexable_page_count,
            internal_3xx_count,
            internal_4xx_count,
            internal_5xx_count,
            schema_page_count,
            location_page_count,
            service_page_count,
            category_page_count,
            product_page_count,
            schema_issue_flags,
            collection_content_issue_flags,
            product_metadata_issue_flags,
            default_title_issue_flags,
            homepage_issue_flags,
            heading_issue_flags,
            heading_outline_score,
            heading_outline_summary,
            heading_pages_analyzed,
            heading_h1_missing_count,
            heading_multiple_h1_count,
            heading_duplicate_h1_count,
            heading_pages_with_h2_count,
            heading_generic_heading_count,
            heading_repeated_heading_count,
            sf_opportunity_score,
            sf_primary_issue_family,
            sf_primary_issue_reason,
            sf_outreach_hooks,
            collection_detection_status,
            collection_detection_confidence,
            collection_main_content,
            collection_main_content_method,
            collection_main_content_confidence,
            collection_above_raw_text,
            collection_below_raw_text,
            collection_above_clean_text,
            collection_below_clean_text,
            collection_best_intro_text,
            collection_best_intro_position,
            collection_best_intro_confidence,
            collection_best_intro_source_type,
            collection_intro_text,
            collection_intro_position,
            collection_intro_status,
            collection_intro_method,
            collection_intro_confidence,
            collection_schema_types,
            collection_schema_types_method,
            collection_schema_types_confidence,
            collection_product_count,
            collection_product_count_method,
            collection_product_count_confidence,
            collection_title_value,
            collection_title_method,
            collection_title_confidence,
            collection_h1_value,
            collection_h1_method,
            collection_h1_confidence,
            title_optimization_status,
            title_optimization_confidence,
            collection_title_rule_family,
            collection_title_rule_match,
            collection_title_rule_confidence,
            collection_title_site_name_match,
            collection_issue_family,
            collection_issue_reason,
            has_internal_errors,
            export_directory
        ) values (
            :root_domain,
            :crawl_mode,
            :resolved_platform_family,
            :resolved_config_path,
            :requested_homepage_url,
            :discovered_final_homepage_url,
            :seed_strategy,
            :seed_count,
            :sitemap_found,
            :sitemap_url,
            :sitemap_source,
            :result_quality,
            :result_reason,
            :checked_at,
            :status,
            :error_message,
            :pages_crawled,
            :homepage_final_url,
            :homepage_status_code,
            :homepage_status_category,
            :homepage_indexability,
            :homepage_title,
            :homepage_meta_description,
            :homepage_canonical,
            :homepage_word_count,
            :redirect_presence,
            :blocked_or_noindex,
            :title_issue_flags,
            :meta_issue_flags,
            :canonical_issue_flags,
            :h1_issue_flags,
            :indexable_page_count,
            :internal_3xx_count,
            :internal_4xx_count,
            :internal_5xx_count,
            :schema_page_count,
            :location_page_count,
            :service_page_count,
            :category_page_count,
            :product_page_count,
            :schema_issue_flags,
            :collection_content_issue_flags,
            :product_metadata_issue_flags,
            :default_title_issue_flags,
            :homepage_issue_flags,
            :heading_issue_flags,
            :heading_outline_score,
            :heading_outline_summary,
            :heading_pages_analyzed,
            :heading_h1_missing_count,
            :heading_multiple_h1_count,
            :heading_duplicate_h1_count,
            :heading_pages_with_h2_count,
            :heading_generic_heading_count,
            :heading_repeated_heading_count,
            :sf_opportunity_score,
            :sf_primary_issue_family,
            :sf_primary_issue_reason,
            :sf_outreach_hooks,
            :collection_detection_status,
            :collection_detection_confidence,
            :collection_main_content,
            :collection_main_content_method,
            :collection_main_content_confidence,
            :collection_above_raw_text,
            :collection_below_raw_text,
            :collection_above_clean_text,
            :collection_below_clean_text,
            :collection_best_intro_text,
            :collection_best_intro_position,
            :collection_best_intro_confidence,
            :collection_best_intro_source_type,
            :collection_intro_text,
            :collection_intro_position,
            :collection_intro_status,
            :collection_intro_method,
            :collection_intro_confidence,
            :collection_schema_types,
            :collection_schema_types_method,
            :collection_schema_types_confidence,
            :collection_product_count,
            :collection_product_count_method,
            :collection_product_count_confidence,
            :collection_title_value,
            :collection_title_method,
            :collection_title_confidence,
            :collection_h1_value,
            :collection_h1_method,
            :collection_h1_confidence,
            :title_optimization_status,
            :title_optimization_confidence,
            :collection_title_rule_family,
            :collection_title_rule_match,
            :collection_title_rule_confidence,
            :collection_title_site_name_match,
            :collection_issue_family,
            :collection_issue_reason,
            :has_internal_errors,
            :export_directory
        )
        on conflict(root_domain) do update set
            crawl_mode=excluded.crawl_mode,
            resolved_platform_family=excluded.resolved_platform_family,
            resolved_config_path=excluded.resolved_config_path,
            requested_homepage_url=excluded.requested_homepage_url,
            discovered_final_homepage_url=excluded.discovered_final_homepage_url,
            seed_strategy=excluded.seed_strategy,
            seed_count=excluded.seed_count,
            sitemap_found=excluded.sitemap_found,
            sitemap_url=excluded.sitemap_url,
            sitemap_source=excluded.sitemap_source,
            result_quality=excluded.result_quality,
            result_reason=excluded.result_reason,
            checked_at=excluded.checked_at,
            status=excluded.status,
            error_message=excluded.error_message,
            pages_crawled=excluded.pages_crawled,
            homepage_final_url=excluded.homepage_final_url,
            homepage_status_code=excluded.homepage_status_code,
            homepage_status_category=excluded.homepage_status_category,
            homepage_indexability=excluded.homepage_indexability,
            homepage_title=excluded.homepage_title,
            homepage_meta_description=excluded.homepage_meta_description,
            homepage_canonical=excluded.homepage_canonical,
            homepage_word_count=excluded.homepage_word_count,
            redirect_presence=excluded.redirect_presence,
            blocked_or_noindex=excluded.blocked_or_noindex,
            title_issue_flags=excluded.title_issue_flags,
            meta_issue_flags=excluded.meta_issue_flags,
            canonical_issue_flags=excluded.canonical_issue_flags,
            h1_issue_flags=excluded.h1_issue_flags,
            indexable_page_count=excluded.indexable_page_count,
            internal_3xx_count=excluded.internal_3xx_count,
            internal_4xx_count=excluded.internal_4xx_count,
            internal_5xx_count=excluded.internal_5xx_count,
            schema_page_count=excluded.schema_page_count,
            location_page_count=excluded.location_page_count,
            service_page_count=excluded.service_page_count,
            category_page_count=excluded.category_page_count,
            product_page_count=excluded.product_page_count,
            schema_issue_flags=excluded.schema_issue_flags,
            collection_content_issue_flags=excluded.collection_content_issue_flags,
            product_metadata_issue_flags=excluded.product_metadata_issue_flags,
            default_title_issue_flags=excluded.default_title_issue_flags,
            homepage_issue_flags=excluded.homepage_issue_flags,
            heading_issue_flags=excluded.heading_issue_flags,
            heading_outline_score=excluded.heading_outline_score,
            heading_outline_summary=excluded.heading_outline_summary,
            heading_pages_analyzed=excluded.heading_pages_analyzed,
            heading_h1_missing_count=excluded.heading_h1_missing_count,
            heading_multiple_h1_count=excluded.heading_multiple_h1_count,
            heading_duplicate_h1_count=excluded.heading_duplicate_h1_count,
            heading_pages_with_h2_count=excluded.heading_pages_with_h2_count,
            heading_generic_heading_count=excluded.heading_generic_heading_count,
            heading_repeated_heading_count=excluded.heading_repeated_heading_count,
            sf_opportunity_score=excluded.sf_opportunity_score,
            sf_primary_issue_family=excluded.sf_primary_issue_family,
            sf_primary_issue_reason=excluded.sf_primary_issue_reason,
            sf_outreach_hooks=excluded.sf_outreach_hooks,
            collection_detection_status=excluded.collection_detection_status,
            collection_detection_confidence=excluded.collection_detection_confidence,
            collection_main_content=excluded.collection_main_content,
            collection_main_content_method=excluded.collection_main_content_method,
            collection_main_content_confidence=excluded.collection_main_content_confidence,
            collection_above_raw_text=excluded.collection_above_raw_text,
            collection_below_raw_text=excluded.collection_below_raw_text,
            collection_above_clean_text=excluded.collection_above_clean_text,
            collection_below_clean_text=excluded.collection_below_clean_text,
            collection_best_intro_text=excluded.collection_best_intro_text,
            collection_best_intro_position=excluded.collection_best_intro_position,
            collection_best_intro_confidence=excluded.collection_best_intro_confidence,
            collection_best_intro_source_type=excluded.collection_best_intro_source_type,
            collection_intro_text=excluded.collection_intro_text,
            collection_intro_position=excluded.collection_intro_position,
            collection_intro_status=excluded.collection_intro_status,
            collection_intro_method=excluded.collection_intro_method,
            collection_intro_confidence=excluded.collection_intro_confidence,
            collection_schema_types=excluded.collection_schema_types,
            collection_schema_types_method=excluded.collection_schema_types_method,
            collection_schema_types_confidence=excluded.collection_schema_types_confidence,
            collection_product_count=excluded.collection_product_count,
            collection_product_count_method=excluded.collection_product_count_method,
            collection_product_count_confidence=excluded.collection_product_count_confidence,
            collection_title_value=excluded.collection_title_value,
            collection_title_method=excluded.collection_title_method,
            collection_title_confidence=excluded.collection_title_confidence,
            collection_h1_value=excluded.collection_h1_value,
            collection_h1_method=excluded.collection_h1_method,
            collection_h1_confidence=excluded.collection_h1_confidence,
            title_optimization_status=excluded.title_optimization_status,
            title_optimization_confidence=excluded.title_optimization_confidence,
            collection_title_rule_family=excluded.collection_title_rule_family,
            collection_title_rule_match=excluded.collection_title_rule_match,
            collection_title_rule_confidence=excluded.collection_title_rule_confidence,
            collection_title_site_name_match=excluded.collection_title_site_name_match,
            collection_issue_family=excluded.collection_issue_family,
            collection_issue_reason=excluded.collection_issue_reason,
            has_internal_errors=excluded.has_internal_errors,
            export_directory=excluded.export_directory
        """,
        payload,
    )


def run_screamingfrog_audits(connection: sqlite3.Connection, *, crawl_mode: str, refresh_existing: bool) -> dict[str, Any]:
    normalized_mode = normalize_screamingfrog_crawl_mode(crawl_mode)
    candidates = selected_tray_screamingfrog_candidates(connection, normalized_mode)
    summary = summarize_screamingfrog_candidates(candidates, skip_existing=not refresh_existing)
    to_run = [item for item in candidates if item["eligible"] and (refresh_existing or not item["already_audited"])]
    if not to_run:
        return {"crawlMode": normalized_mode, "summary": summary, "results": []}

    results: list[dict[str, Any]] = []
    for item in to_run:
        payload = run_screamingfrog_crawl(
            item["root_domain"],
            normalized_mode,
            resolved_platform_family=item["resolved_platform_family"],
        )
        persist_screamingfrog_snapshot(connection, payload)
        results.append(
            {
                "root_domain": item["root_domain"],
                "status": payload["status"],
                "error_message": payload["error_message"],
                "resolved_platform_family": payload["resolved_platform_family"],
                "pages_crawled": payload["pages_crawled"],
                "homepage_status_category": payload["homepage_status_category"],
            }
        )

    connection.commit()
    return {"crawlMode": normalized_mode, "summary": summary, "results": results}


def build_filter_payload_from_row(row: sqlite3.Row) -> dict[str, Any]:
    filters = decode_json_text(row["filters_json"]) or {}
    sort = decode_json_text(row["sort_json"]) or {}
    normalized_query = normalize_lead_filters(filters.get("query", filters))
    return {
        "id": row["id"],
        "name": row["name"],
        "isBuiltin": bool(row["is_builtin"]),
        "filters": normalized_query,
        "visibleColumns": filters.get("visibleColumns", DEFAULT_VISIBLE_COLUMNS),
        "group": filters.get("group", "Custom"),
        "description": filters.get("description", ""),
        "order": filters.get("order", 999),
        "sort": sort,
        "createdAt": row["created_at"],
        "updatedAt": row["updated_at"],
    }


def build_analytics_payload(rows: list[sqlite3.Row], migrations: list[sqlite3.Row]) -> dict[str, Any]:
    countries = Counter()
    tiers = Counter()
    current_platforms = Counter()
    sales_buckets = Counter()
    recent_migration_count = 0
    confirmed_domain = 0
    probable_domain = 0
    confirmed_cms = 0
    possible_cms = 0

    filtered_root_domains = set()
    for row in rows:
        filtered_root_domains.add(row["root_domain"])
        countries[row["country"]] += 1
        tiers[row["priority_tier"]] += 1

        platforms = split_pipe(row["current_platforms"]) or split_pipe(row["likely_current_platforms"])
        for platform in platforms:
            current_platforms[platform] += 1

        for bucket in split_pipe(row["sales_buckets"]):
            sales_buckets[bucket] += 1

        if "recent_migration_signal" in split_pipe(row["sales_buckets"]):
            recent_migration_count += 1
        if row["domain_migration_status"] == "confirmed":
            confirmed_domain += 1
        elif row["domain_migration_status"] in {"probable", "network"}:
            probable_domain += 1
        if row["cms_migration_status"] == "confirmed":
            confirmed_cms += 1
        elif row["cms_migration_status"] in {"possible", "historic"}:
            possible_cms += 1

    corridors = Counter()
    for row in migrations:
        if row["root_domain"] in filtered_root_domains:
            corridors[f"{row['old_platform']}->{row['new_platform']}"] += 1

    return {
        "kpis": {
            "filteredLeads": len(rows),
            "priorityAB": tiers.get("A", 0) + tiers.get("B", 0),
            "recentMigrations": recent_migration_count,
            "confirmedDomainMigrations": confirmed_domain,
            "possibleDomainMigrations": probable_domain,
            "confirmedCmsMigrations": confirmed_cms,
            "possibleCmsMigrations": possible_cms,
        },
        "countryMix": [{"label": label, "count": count} for label, count in countries.items()],
        "tierMix": [{"label": label, "count": count} for label, count in tiers.items()],
        "currentPlatformMix": [
            {"label": label, "count": count}
            for label, count in current_platforms.most_common(8)
        ],
        "salesBucketMix": [
            {"label": label, "count": count}
            for label, count in sales_buckets.most_common(10)
        ],
        "topCorridors": [
            {"label": label.replace("->", " → "), "count": count}
            for label, count in corridors.most_common(8)
        ],
    }


def build_export_tray_payload(connection: sqlite3.Connection) -> dict[str, Any]:
    rows = connection.execute(
        """
        select leads.root_domain, leads.company, leads.country, leads.priority_tier, leads.sales_buckets, tray.added_at
        from export_tray_items tray
        join data.leads leads on leads.root_domain = tray.root_domain
        order by tray.added_at desc
        """
    ).fetchall()
    country_mix = Counter()
    bucket_mix = Counter()
    for row in rows:
        country_mix[row["country"]] += 1
        for bucket in split_pipe(row["sales_buckets"]):
            bucket_mix[bucket] += 1

    return {
        "count": len(rows),
        "rootDomains": [row["root_domain"] for row in rows],
        "items": [
            {
                "root_domain": row["root_domain"],
                "company": row["company"],
                "country": row["country"],
                "priority_tier": row["priority_tier"],
                "added_at": row["added_at"],
            }
            for row in rows[:50]
        ],
        "countryMix": [{"label": label, "count": count} for label, count in country_mix.items()],
        "bucketMix": [{"label": label, "count": count} for label, count in bucket_mix.most_common(8)],
    }


def build_timeline_cohort_payload(rows: list[sqlite3.Row], granularity: str, timeline_date_field: str) -> dict[str, Any]:
    period_counts = Counter()
    platform_counts = Counter()
    platform_period_counts: dict[str, Counter[str]] = defaultdict(Counter)
    unique_domains: set[str] = set()
    date_values: list[str] = []

    for row in rows:
        date_value = row["date_value"]
        if not date_value:
            continue
        try:
            period = period_label(date_value, granularity)
        except ValueError:
            continue
        period_counts[period] += 1
        platform_counts[row["platform"]] += 1
        platform_period_counts[row["platform"]][period] += 1
        unique_domains.add(row["root_domain"])
        date_values.append(date_value)

    ordered_periods = sorted(period_counts.items(), key=lambda item: item[0])
    ordered_period_labels = [period for period, _count in ordered_periods]
    ordered_platforms = [platform for platform, _count in platform_counts.most_common()]

    return {
        "summary": {
            "totalStarts": sum(period_counts.values()),
            "uniqueDomains": len(unique_domains),
            "periodCount": len(ordered_periods),
            "firstPeriod": ordered_periods[0][0] if ordered_periods else None,
            "lastPeriod": ordered_periods[-1][0] if ordered_periods else None,
        },
        "series": [{"period": period, "count": count} for period, count in ordered_periods],
        "seriesByPlatform": [
            {
                "platform": platform,
                "points": [
                    {"period": period, "count": platform_period_counts[platform].get(period, 0)}
                    for period in ordered_period_labels
                ],
            }
            for platform in ordered_platforms
        ],
        "technologyBreakdown": [{"platform": platform, "count": count} for platform, count in platform_counts.most_common()],
        "availableRange": {
            "dateField": normalize_timeline_date_field(timeline_date_field),
            "minDate": min(date_values) if date_values else None,
            "maxDate": max(date_values) if date_values else None,
        },
    }


def fetch_filtered_rows(
    connection: sqlite3.Connection,
    *,
    filters: Mapping[str, Any],
) -> tuple[str, dict[str, Any], list[sqlite3.Row]]:
    migration_join_sql, migration_select_sql = build_migration_join_and_select_sql()
    seranking_join_sql, _seranking_select_sql = build_seranking_join_and_select_sql()
    site_status_join_sql, _site_status_select_sql = build_site_status_join_and_select_sql()
    screamingfrog_join_sql, _screamingfrog_select_sql = build_screamingfrog_join_and_select_sql()
    normalized_filters = normalize_lead_filters(filters)
    where, params = build_lead_filters(
        normalized_filters,
        apply_timeline_match=not bool(normalized_filters["timeline_platforms"]),
    )
    base_join_sql = filter_base_joins(normalized_filters)
    timeline_join_sql, _matched_select_sql = build_timeline_join_and_select_sql(
        normalized_filters["timeline_platforms"],
        normalized_filters["timeline_event_types"],
        normalized_filters["timeline_date_field"],
        normalized_filters["timeline_seen_from"],
        normalized_filters["timeline_seen_to"],
        params,
    )
    rows = connection.execute(
        f"""
        select leads.*, {migration_select_sql}
        from leads
        {base_join_sql}
        {migration_join_sql}
        {seranking_join_sql}
        {site_status_join_sql}
        {screamingfrog_join_sql}
        {timeline_join_sql}
        where {where}
        """,
        params,
    ).fetchall()
    return where, params, rows


@app.get("/api/health")
def health() -> dict[str, Any]:
    return health_snapshot()


@app.get("/api/summary")
def summary() -> dict[str, Any]:
    return json.loads(SUMMARY_PATH.read_text(encoding="utf-8"))


@app.get("/api/filter-options")
def filter_options(request: FastAPIRequest) -> dict[str, Any]:
    payload = json.loads(FILTER_OPTIONS_PATH.read_text(encoding="utf-8"))
    filters = extract_lead_query_args(request)
    if filters_have_scope(filters):
        payload.update(scoped_dynamic_filter_options(filters))
    else:
        payload.update(dynamic_filter_options())
    payload["domainMigrationStatuses"] = DOMAIN_MIGRATION_STATUSES
    payload["domainConfidenceBands"] = DOMAIN_CONFIDENCE_BANDS
    payload["domainFingerprintStrengths"] = DOMAIN_FINGERPRINT_STRENGTHS
    payload["domainTldRelationships"] = DOMAIN_TLD_RELATIONSHIPS
    payload["cmsMigrationStatuses"] = CMS_MIGRATION_STATUSES
    payload["cmsConfidenceLevels"] = CMS_CONFIDENCE_LEVELS
    payload["seRankingAnalysisTypes"] = SERANKING_ANALYSIS_TYPES
    payload["seRankingOutcomeFlags"] = SERANKING_OUTCOME_FLAGS
    payload["siteStatusCategories"] = SITE_STATUS_CATEGORIES
    payload["screamingFrogStatuses"] = SCREAMINGFROG_AUDIT_STATUSES
    payload["screamingFrogHomepageStatuses"] = SCREAMINGFROG_HOMEPAGE_STATUSES
    payload["screamingFrogTitleFlags"] = SCREAMINGFROG_TITLE_FLAGS
    payload["screamingFrogMetaFlags"] = SCREAMINGFROG_META_FLAGS
    payload["screamingFrogCanonicalFlags"] = SCREAMINGFROG_CANONICAL_FLAGS
    return payload


@app.get("/api/presets")
def list_presets() -> dict[str, Any]:
    connection = get_connection()
    try:
        rows = connection.execute(
            "select * from state.saved_presets order by is_builtin desc, updated_at desc, lower(name) asc"
        ).fetchall()
        presets = [build_filter_payload_from_row(row) for row in rows]
        presets.sort(key=lambda item: (item["group"], item["order"], item["name"].lower()))
        return {"items": presets}
    finally:
        connection.close()


@app.post("/api/presets")
def create_preset(payload: PresetPayload) -> dict[str, Any]:
    connection = get_connection()
    try:
        preset_id = f"custom_{uuid.uuid4().hex}"
        created_at = now_iso()
        filters = {
            "query": normalize_lead_filters(payload.filters),
            "visibleColumns": payload.visible_columns,
            "group": "Custom",
            "description": "Saved custom view",
            "order": 999,
        }
        connection.execute(
            """
            insert into state.saved_presets (id, name, is_builtin, filters_json, sort_json, created_at, updated_at)
            values (?, ?, 0, ?, ?, ?, ?)
            """,
            (preset_id, payload.name.strip(), json_text(filters), json_text(payload.sort), created_at, created_at),
        )
        connection.commit()
        row = connection.execute("select * from state.saved_presets where id = ?", (preset_id,)).fetchone()
        return build_filter_payload_from_row(row)
    finally:
        connection.close()


@app.put("/api/presets/{preset_id}")
def update_preset(preset_id: str, payload: PresetPayload) -> dict[str, Any]:
    connection = get_connection()
    try:
        existing = connection.execute("select * from state.saved_presets where id = ?", (preset_id,)).fetchone()
        if existing is None:
            raise HTTPException(status_code=404, detail="Preset not found")
        if existing["is_builtin"]:
            raise HTTPException(status_code=400, detail="Built-in presets are read-only")

        filters = {
            "query": normalize_lead_filters(payload.filters),
            "visibleColumns": payload.visible_columns,
            "group": "Custom",
            "description": "Saved custom view",
            "order": 999,
        }
        connection.execute(
            """
            update state.saved_presets
            set name = ?, filters_json = ?, sort_json = ?, updated_at = ?
            where id = ?
            """,
            (payload.name.strip(), json_text(filters), json_text(payload.sort), now_iso(), preset_id),
        )
        connection.commit()
        row = connection.execute("select * from state.saved_presets where id = ?", (preset_id,)).fetchone()
        return build_filter_payload_from_row(row)
    finally:
        connection.close()


@app.delete("/api/presets/{preset_id}")
def delete_preset(preset_id: str) -> dict[str, bool]:
    connection = get_connection()
    try:
        existing = connection.execute("select * from state.saved_presets where id = ?", (preset_id,)).fetchone()
        if existing is None:
            raise HTTPException(status_code=404, detail="Preset not found")
        if existing["is_builtin"]:
            raise HTTPException(status_code=400, detail="Built-in presets are read-only")
        connection.execute("delete from state.saved_presets where id = ?", (preset_id,))
        connection.commit()
        return {"ok": True}
    finally:
        connection.close()


@app.get("/api/export-tray")
def get_export_tray() -> dict[str, Any]:
    connection = get_state_connection()
    try:
        return build_export_tray_payload(connection)
    finally:
        connection.close()


@app.post("/api/export-tray/items")
def add_export_tray_items(payload: TrayMutationPayload) -> dict[str, Any]:
    connection = get_state_connection()
    try:
        created_at = now_iso()
        for root_domain in payload.root_domains:
            connection.execute(
                """
                insert into export_tray_items (root_domain, added_at)
                values (?, ?)
                on conflict(root_domain) do update set added_at = excluded.added_at
                """,
                (root_domain, created_at),
            )
        connection.commit()
        return build_export_tray_payload(connection)
    finally:
        connection.close()


@app.post("/api/export-tray/select-filtered")
def select_filtered_export_tray(request: FastAPIRequest) -> dict[str, Any]:
    filters = extract_lead_query_args(request)

    connection = get_connection()
    try:
        before_count = connection.execute("select count(*) from state.export_tray_items").fetchone()[0]
        migration_join_sql, _migration_select_sql = build_migration_join_and_select_sql()
        seranking_join_sql, _seranking_select_sql = build_seranking_join_and_select_sql()
        site_status_join_sql, _site_status_select_sql = build_site_status_join_and_select_sql()
        screamingfrog_join_sql, _screamingfrog_select_sql = build_screamingfrog_join_and_select_sql()
        where, params = build_lead_filters(filters, apply_timeline_match=not bool(filters["timeline_platforms"]))
        base_join_sql = filter_base_joins(filters)
        timeline_join_sql, _matched_select_sql = build_timeline_join_and_select_sql(
            filters["timeline_platforms"],
            filters["timeline_event_types"],
            filters["timeline_date_field"],
            filters["timeline_seen_from"],
            filters["timeline_seen_to"],
            params,
        )
        matched_count = connection.execute(
            f"""
            select count(*)
            from leads
            {base_join_sql}
            {migration_join_sql}
            {seranking_join_sql}
            {site_status_join_sql}
            {screamingfrog_join_sql}
            {timeline_join_sql}
            where {where}
            """,
            params,
        ).fetchone()[0]
        connection.execute(
            f"""
            insert into state.export_tray_items (root_domain, added_at)
            select leads.root_domain, :added_at
            from leads
            {base_join_sql}
            {migration_join_sql}
            {seranking_join_sql}
            {site_status_join_sql}
            {screamingfrog_join_sql}
            {timeline_join_sql}
            where {where}
            on conflict(root_domain) do nothing
            """,
            {**params, "added_at": now_iso()},
        )
        connection.commit()
    finally:
        connection.close()

    state_connection = get_state_connection()
    try:
        payload = build_export_tray_payload(state_connection)
        payload["matchedCount"] = matched_count
        payload["addedCount"] = max(payload["count"] - before_count, 0)
        return payload
    finally:
        state_connection.close()


@app.delete("/api/export-tray/items/{root_domain:path}")
def delete_export_tray_item(root_domain: str) -> dict[str, Any]:
    connection = get_state_connection()
    try:
        connection.execute("delete from export_tray_items where root_domain = ?", (root_domain,))
        connection.commit()
        return build_export_tray_payload(connection)
    finally:
        connection.close()


@app.post("/api/export-tray/clear")
def clear_export_tray() -> dict[str, Any]:
    connection = get_state_connection()
    try:
        connection.execute("delete from export_tray_items")
        connection.commit()
        return build_export_tray_payload(connection)
    finally:
        connection.close()


@app.get("/api/seranking/summary")
def seranking_summary(request: FastAPIRequest, analysis_type: str = "cms_migration", use_filtered_view: bool = False) -> dict[str, Any]:
    connection = get_connection()
    try:
        normalized_type = normalize_seranking_analysis_type(analysis_type)
        candidates = (
            filtered_view_analysis_candidates(connection, normalized_type, filters=extract_lead_query_args(request))
            if use_filtered_view
            else selected_tray_analysis_candidates(connection, normalized_type)
        )
        return {
            "analysisType": normalized_type,
            "summary": summarize_seranking_candidates(candidates, skip_existing=True),
        }
    finally:
        connection.close()


@app.post("/api/seranking/analyze")
def seranking_analyze(payload: SeRankingRunPayload) -> dict[str, Any]:
    connection = get_connection()
    try:
        normalized_type = normalize_seranking_analysis_type(payload.analysis_type)
        candidates = (
            filtered_view_analysis_candidates(connection, normalized_type, filters=payload.filters)
            if payload.use_filtered_view
            else selected_tray_analysis_candidates(connection, normalized_type)
        )
        summary = summarize_seranking_candidates(candidates, skip_existing=True)
        if not payload.confirm:
            return {"analysisType": normalized_type, "summary": summary, "results": []}
        result = run_seranking_analysis(
            connection,
            normalized_type,
            refresh_existing=False,
            filters=payload.filters,
            use_filtered_view=payload.use_filtered_view,
        )
        return {"analysisType": normalized_type, **result}
    finally:
        connection.close()


@app.post("/api/seranking/refresh")
def seranking_refresh(payload: SeRankingRefreshPayload) -> dict[str, Any]:
    connection = get_connection()
    try:
        normalized_type = normalize_seranking_analysis_type(payload.analysis_type)
        result = run_seranking_analysis(
            connection,
            normalized_type,
            refresh_existing=True,
            filters=payload.filters,
            use_filtered_view=payload.use_filtered_view,
        )
        return {"analysisType": normalized_type, **result}
    finally:
        connection.close()


@app.post("/api/seranking/manual/preview")
def seranking_manual_preview(payload: SeRankingManualPayload) -> dict[str, Any]:
    connection = get_connection()
    try:
        first_month, second_month = validate_manual_months(payload.first_month, payload.second_month)
        candidates = manual_analysis_candidates(
            connection,
            root_domains=payload.root_domains,
            use_selected_tray=payload.use_selected_tray,
            first_month=first_month,
            second_month=second_month,
        )
        return {
            "analysisType": "manual_comparison",
            "analysisMode": "manual",
            "firstMonth": first_month,
            "secondMonth": second_month,
            "summary": summarize_seranking_candidates(candidates, skip_existing=False),
        }
    finally:
        connection.close()


@app.post("/api/seranking/manual/run")
def seranking_manual_run(payload: SeRankingManualPayload) -> dict[str, Any]:
    connection = get_connection()
    try:
        result = run_manual_seranking_analysis(
            connection,
            root_domains=payload.root_domains,
            use_selected_tray=payload.use_selected_tray,
            first_month=payload.first_month,
            second_month=payload.second_month,
        )
        return result
    finally:
        connection.close()


@app.get("/api/site-status/summary")
def site_status_summary() -> dict[str, Any]:
    connection = get_connection()
    try:
        candidates = selected_tray_site_status_candidates(connection)
        return {"summary": summarize_site_status_candidates(candidates, skip_existing=True)}
    finally:
        connection.close()


@app.post("/api/site-status/analyze")
def site_status_analyze(payload: SiteStatusRunPayload) -> dict[str, Any]:
    connection = get_connection()
    try:
        candidates = selected_tray_site_status_candidates(connection)
        summary = summarize_site_status_candidates(candidates, skip_existing=True)
        if not payload.confirm:
            return {"summary": summary, "results": []}
        return run_site_status_checks(connection, refresh_existing=False)
    finally:
        connection.close()


@app.post("/api/site-status/refresh")
def site_status_refresh() -> dict[str, Any]:
    connection = get_connection()
    try:
        return run_site_status_checks(connection, refresh_existing=True)
    finally:
        connection.close()


@app.get("/api/screamingfrog/summary")
def screamingfrog_summary(crawl_mode: str = "bounded_audit") -> dict[str, Any]:
    connection = get_connection()
    try:
        normalized_mode = normalize_screamingfrog_crawl_mode(crawl_mode)
        candidates = selected_tray_screamingfrog_candidates(connection, normalized_mode)
        active_batch = fetch_active_screamingfrog_batch(connection, tray_domains=[item["root_domain"] for item in candidates])
        if active_batch and active_batch.get("isActive"):
            ensure_screamingfrog_worker()
        return {
            "crawlMode": normalized_mode,
            "summary": summarize_screamingfrog_candidates(candidates, skip_existing=True),
            "jobBatch": fetch_active_screamingfrog_batch(connection, tray_domains=[item["root_domain"] for item in candidates]),
        }
    finally:
        connection.close()


@app.post("/api/screamingfrog/analyze")
def screamingfrog_analyze(payload: ScreamingFrogRunPayload) -> dict[str, Any]:
    connection = get_connection()
    try:
        normalized_mode = normalize_screamingfrog_crawl_mode(payload.crawl_mode)
        candidates = selected_tray_screamingfrog_candidates(connection, normalized_mode)
        summary = summarize_screamingfrog_candidates(candidates, skip_existing=True)
        if not payload.confirm:
            return {
                "crawlMode": normalized_mode,
                "summary": summary,
                "results": [],
                "jobBatch": fetch_active_screamingfrog_batch(connection, tray_domains=[item["root_domain"] for item in candidates]),
            }
        return enqueue_screamingfrog_jobs(connection, crawl_mode=normalized_mode, refresh_existing=False)
    finally:
        connection.close()


@app.post("/api/screamingfrog/refresh")
def screamingfrog_refresh(payload: ScreamingFrogRunPayload) -> dict[str, Any]:
    connection = get_connection()
    try:
        normalized_mode = normalize_screamingfrog_crawl_mode(payload.crawl_mode)
        return enqueue_screamingfrog_jobs(connection, crawl_mode=normalized_mode, refresh_existing=True)
    finally:
        connection.close()


@app.get("/api/screamingfrog/jobs/{batch_id}")
def screamingfrog_job_status(batch_id: str) -> dict[str, Any]:
    connection = get_connection()
    try:
        batch = fetch_screamingfrog_batch(connection, batch_id)
        if batch is None:
            raise HTTPException(status_code=404, detail="Screaming Frog job batch not found")
        if batch.get("isActive"):
            ensure_screamingfrog_worker()
            batch = fetch_screamingfrog_batch(connection, batch_id) or batch
        return batch
    finally:
        connection.close()


@app.post("/api/screamingfrog/jobs/{batch_id}/stop")
def screamingfrog_job_stop(batch_id: str) -> dict[str, Any]:
    batch = stop_screamingfrog_batch(batch_id)
    if batch is None:
        raise HTTPException(status_code=404, detail="Screaming Frog job batch not found")
    return batch


@app.get("/api/screamingfrog/audit/open")
def screamingfrog_audit_open(root_domain: str) -> HTMLResponse:
    connection = get_connection()
    try:
        export_directory = resolve_screamingfrog_export_directory(connection, root_domain)
        snapshot = connection.execute(
            """
            select *
            from state.screamingfrog_audit_snapshots
            where root_domain = ?
            """,
            (root_domain,),
        ).fetchone()
        files = build_screamingfrog_audit_file_entries(export_directory, root_domain)
        tabs = build_screamingfrog_audit_tabs(export_directory)
    finally:
        connection.close()
    if snapshot is None:
        raise HTTPException(status_code=404, detail="No Screaming Frog audit snapshot saved for this domain")
    snapshot_data = {key: unescape(str(value or "")) for key, value in dict(snapshot).items()}
    summary_cards = [
        ("Audit status", humanize_token(snapshot_data.get("status", "unknown"))),
        ("Result quality", humanize_token(snapshot_data.get("result_quality", "unknown"))),
        ("Resolved config", humanize_token(snapshot_data.get("resolved_platform_family", "generic"))),
        ("Checked", snapshot_data.get("checked_at", "") or "—"),
        ("Pages crawled", snapshot_data.get("pages_crawled", "0") or "0"),
        ("Seed strategy", humanize_token(snapshot_data.get("seed_strategy", "unknown"))),
        ("Seed count", snapshot_data.get("seed_count", "0") or "0"),
        ("Opportunity score", snapshot_data.get("sf_opportunity_score", "0") or "0"),
    ]
    issue_cards = [
        ("Primary issue", humanize_token(snapshot_data.get("sf_primary_issue_family", "none"))),
        ("Primary reason", snapshot_data.get("sf_primary_issue_reason", "") or "No primary issue reason"),
        ("Category pages", snapshot_data.get("category_page_count", "0") or "0"),
        ("Product pages", snapshot_data.get("product_page_count", "0") or "0"),
        ("Heading summary", snapshot_data.get("heading_outline_summary", "") or "No heading summary"),
        ("Heading flags", ", ".join(humanize_token(item) for item in split_pipe(snapshot_data.get("heading_issue_flags", ""))) or "None"),
        ("Internal errors", f"{snapshot_data.get('internal_4xx_count', '0')} 4xx · {snapshot_data.get('internal_5xx_count', '0')} 5xx"),
        ("Schema flags", ", ".join(humanize_token(item) for item in split_pipe(snapshot_data.get("schema_issue_flags", ""))) or "None"),
        ("Title flags", ", ".join(humanize_token(item) for item in split_pipe(snapshot_data.get("title_issue_flags", ""))) or "None"),
        ("Meta flags", ", ".join(humanize_token(item) for item in split_pipe(snapshot_data.get("meta_issue_flags", ""))) or "None"),
        ("Canonical flags", ", ".join(humanize_token(item) for item in split_pipe(snapshot_data.get("canonical_issue_flags", ""))) or "None"),
        ("H1 flags", ", ".join(humanize_token(item) for item in split_pipe(snapshot_data.get("h1_issue_flags", ""))) or "None"),
    ]
    collection_cards = [
        ("Detection status", humanize_token(snapshot_data.get("collection_detection_status", "unknown"))),
        ("Detection confidence", snapshot_data.get("collection_detection_confidence", "0") or "0"),
        ("Intro status", humanize_token(snapshot_data.get("collection_intro_status", "unknown"))),
        ("Intro confidence", snapshot_data.get("collection_best_intro_confidence", snapshot_data.get("collection_intro_confidence", "0")) or "0"),
        ("Intro position", humanize_token(snapshot_data.get("collection_intro_position", "unknown"))),
        ("Title optimisation", humanize_token(snapshot_data.get("title_optimization_status", "unknown"))),
        ("Title confidence", snapshot_data.get("title_optimization_confidence", "0") or "0"),
        ("Collection issue", humanize_token(snapshot_data.get("collection_issue_family", "none"))),
        ("Schema types", ", ".join(humanize_token(item) for item in split_pipe(snapshot_data.get("collection_schema_types", ""))) or "None"),
        ("Product count", snapshot_data.get("collection_product_count", "0") or "0"),
        ("Title rule", humanize_token(snapshot_data.get("collection_title_rule_family", "unknown"))),
    ]
    hooks = split_pipe_text(snapshot_data.get("sf_outreach_hooks", ""))
    raw_rows = [
        {
            "File": entry["name"],
            "Size": f"{int(entry['size'])} bytes",
            "Modified": entry["modified_at"],
            "Open": entry["download_url"],
        }
        for entry in files
    ]
    tabs_payload = [
        {
            "id": "overview",
            "label": "Overview",
            "count": 0,
            "isOverview": True,
            "summaryCards": summary_cards,
            "discoveryCards": [
                ("Requested homepage", snapshot_data.get("requested_homepage_url", "") or "—"),
                ("Final homepage", snapshot_data.get("discovered_final_homepage_url", "") or "—"),
                ("Redirect detected", "Yes" if snapshot_data.get("requested_homepage_url", "") != snapshot_data.get("discovered_final_homepage_url", "") and snapshot_data.get("discovered_final_homepage_url", "") else "No"),
                ("Sitemap found", "Yes" if snapshot_data.get("sitemap_found", "") in {"1", "True", "true"} else "No"),
                ("Sitemap source", snapshot_data.get("sitemap_source", "") or "—"),
                ("Sitemap URL", snapshot_data.get("sitemap_url", "") or "—"),
            ],
            "issueCards": issue_cards,
            "collectionCards": collection_cards,
            "collectionIntroText": snapshot_data.get("collection_best_intro_text", "") or snapshot_data.get("collection_intro_text", ""),
            "collectionIssueReason": snapshot_data.get("collection_issue_reason", ""),
            "hooks": hooks,
            "folderPath": str(export_directory),
            "leadJsonUrl": f"/api/leads/{escape(root_domain)}",
            "firstRawFileUrl": files[0]["download_url"] if files else "",
        },
        *tabs,
        {
            "id": "raw-files",
            "label": "Raw files",
            "count": len(raw_rows),
            "empty": "No raw export files were saved for this crawl.",
            "columns": ["File", "Size", "Modified", "Open"],
            "rows": raw_rows,
            "isRawFiles": True,
        },
    ]
    tabs_json = json.dumps(tabs_payload)
    html = f"""
    <!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <title>Screaming Frog Audit · {escape(root_domain)}</title>
        <style>
          :root {{ color-scheme: light; }}
          * {{ box-sizing: border-box; }}
          body {{ font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 0; color: #111827; background: linear-gradient(180deg, #f8fafc 0%, #eef2ff 100%); }}
          header {{ position: sticky; top: 0; z-index: 10; background: rgba(248,250,252,0.96); backdrop-filter: blur(12px); border-bottom: 1px solid #dbe4f0; }}
          .header-inner {{ max-width: 1400px; margin: 0 auto; padding: 18px 24px; display: flex; justify-content: space-between; align-items: end; gap: 20px; }}
          .eyebrow {{ letter-spacing: .12em; text-transform: uppercase; font-size: 12px; color: #64748b; margin-bottom: 6px; }}
          h1 {{ margin: 0; font-size: 28px; }}
          .subline {{ color: #475569; margin-top: 6px; }}
          .shell {{ max-width: 1400px; margin: 0 auto; padding: 24px; }}
          .actions {{ display: flex; gap: 10px; flex-wrap: wrap; }}
          .button {{ display: inline-flex; align-items: center; gap: 8px; padding: 10px 14px; border: 1px solid #cbd5e1; border-radius: 999px; color: #111827; background: #fff; text-decoration: none; font-weight: 600; }}
          .button.primary {{ background: #2563eb; color: white; border-color: #2563eb; }}
          .button:hover {{ text-decoration: none; box-shadow: 0 6px 18px rgba(37,99,235,0.12); }}
          .tab-row {{ display: flex; gap: 10px; flex-wrap: wrap; margin: 0 0 20px; }}
          .tab-button {{ border: 1px solid #cbd5e1; background: #fff; color: #1e293b; border-radius: 999px; padding: 10px 14px; font-weight: 700; cursor: pointer; }}
          .tab-button.active {{ background: #111827; color: white; border-color: #111827; }}
          .panel {{ background: white; border: 1px solid #dbe4f0; border-radius: 24px; padding: 24px; box-shadow: 0 20px 60px rgba(15,23,42,0.06); }}
          .grid {{ display: grid; gap: 14px; }}
          .card-grid {{ grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); margin-bottom: 18px; }}
          .card {{ border: 1px solid #e2e8f0; border-radius: 18px; padding: 16px; background: #f8fafc; }}
          .card .label {{ font-size: 12px; text-transform: uppercase; letter-spacing: .08em; color: #64748b; margin-bottom: 8px; }}
          .card .value {{ font-weight: 700; line-height: 1.35; }}
          .hook-box {{ border: 1px solid #bfdbfe; background: #eff6ff; border-radius: 18px; padding: 18px; margin-top: 18px; }}
          .hook-box h3 {{ margin: 0 0 8px; }}
          .hook-box ul {{ margin: 0; padding-left: 18px; }}
          .section-title {{ margin: 0 0 14px; font-size: 18px; }}
          .table-wrap {{ overflow: auto; border: 1px solid #e2e8f0; border-radius: 18px; }}
          table {{ width: 100%; border-collapse: collapse; min-width: 860px; }}
          th, td {{ padding: 12px 14px; border-bottom: 1px solid #e2e8f0; text-align: left; vertical-align: top; }}
          th {{ font-size: 12px; text-transform: uppercase; letter-spacing: .06em; color: #64748b; background: #f8fafc; position: sticky; top: 0; }}
          td {{ font-size: 14px; }}
          .muted {{ color: #64748b; }}
          .badge {{ display: inline-flex; align-items: center; padding: 4px 10px; border-radius: 999px; font-size: 12px; font-weight: 700; background: #e2e8f0; color: #334155; }}
          .empty {{ padding: 24px; border: 1px dashed #cbd5e1; border-radius: 18px; color: #64748b; background: #f8fafc; }}
          code {{ background: #f3f4f6; padding: 2px 6px; border-radius: 6px; }}
        </style>
      </head>
      <body>
        <header>
          <div class="header-inner">
            <div>
              <div class="eyebrow">Screaming Frog audit workspace</div>
              <h1>{escape(root_domain)}</h1>
              <div class="subline">Sales-friendly review of the saved local crawl, with overview first and raw files second.</div>
            </div>
            <div class="actions">
              <a class="button primary" href="/api/leads/{escape(root_domain)}" target="_blank" rel="noopener noreferrer">Open lead JSON</a>
              <a class="button" href="{escape(files[0]['download_url']) if files else '#'}" target="_blank" rel="noopener noreferrer">Open raw file</a>
            </div>
          </div>
        </header>
        <main class="shell">
          <div class="tab-row" id="tab-row"></div>
          <section class="panel" id="tab-panel"></section>
        </main>
        <script>
          const tabs = {tabs_json};
          const tabRow = document.getElementById('tab-row');
          const tabPanel = document.getElementById('tab-panel');
          function esc(value) {{
            return String(value ?? '').replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('\"', '&quot;');
          }}
          function badge(value) {{
            return `<span class="badge">${{esc(value)}}</span>`;
          }}
          function renderOverview(tab) {{
            const summary = (tab.summaryCards || []).map(([label, value]) => `<div class="card"><div class="label">${{esc(label)}}</div><div class="value">${{esc(value)}}</div></div>`).join('');
            const discovery = (tab.discoveryCards || []).map(([label, value]) => `<div class="card"><div class="label">${{esc(label)}}</div><div class="value">${{esc(value)}}</div></div>`).join('');
            const issues = (tab.issueCards || []).map(([label, value]) => `<div class="card"><div class="label">${{esc(label)}}</div><div class="value">${{esc(value)}}</div></div>`).join('');
            const collections = (tab.collectionCards || []).map(([label, value]) => `<div class="card"><div class="label">${{esc(label)}}</div><div class="value">${{esc(value)}}</div></div>`).join('');
            const hooks = (tab.hooks || []).length
              ? `<div class="hook-box"><h3>What a salesperson can say</h3><ul>${{tab.hooks.map((hook) => `<li>${{esc(hook)}}</li>`).join('')}}</ul></div>`
              : `<div class="hook-box"><h3>What a salesperson can say</h3><p class="muted">No outreach hooks were derived from this crawl.</p></div>`;
            return `
              <h2 class="section-title">Overview</h2>
              <div class="grid card-grid">${{summary}}</div>
              <h3 class="section-title">Discovery summary</h3>
              <div class="grid card-grid">${{discovery}}</div>
              <h3 class="section-title">Issue summary</h3>
              <div class="grid card-grid">${{issues}}</div>
              <h3 class="section-title">Collection intelligence</h3>
              <div class="grid card-grid">${{collections}}</div>
              ${{tab.collectionIntroText || tab.collectionIssueReason ? `<div class="hook-box"><h3>Collection page reading</h3>${{tab.collectionIssueReason ? `<p><strong>${{esc(tab.collectionIssueReason)}}</strong></p>` : ''}}${{tab.collectionIntroText ? `<p class="muted">${{esc(tab.collectionIntroText)}}</p>` : '<p class="muted">No collection intro copy was extracted.</p>'}}</div>` : ''}}
              ${{hooks}}
              <div class="hook-box">
                <h3>Quick actions</h3>
                <div class="actions">
                  <a class="button" href="${{esc(tab.leadJsonUrl)}}" target="_blank" rel="noopener noreferrer">Open lead JSON</a>
                  ${{tab.firstRawFileUrl ? `<a class="button" href="${{esc(tab.firstRawFileUrl)}}" target="_blank" rel="noopener noreferrer">Open raw file</a>` : ''}}
                </div>
                <p class="muted"><strong>Local folder:</strong> <code>${{esc(tab.folderPath || '')}}</code></p>
              </div>
            `;
          }}
          function renderTable(tab) {{
            if (!(tab.rows || []).length) {{
              return `<div class="empty">${{esc(tab.empty || 'No rows available for this tab.')}}</div>`;
            }}
            const headers = (tab.columns || []).map((column) => `<th>${{esc(column)}}</th>`).join('');
            const body = tab.rows.map((row) => `<tr>${{(tab.columns || []).map((column) => {{
              const value = row[column] ?? '';
              if (column === 'Issue' || column === 'Status') {{
                return `<td>${{badge(value || '—')}}</td>`;
              }}
              if (column === 'Open' && value) {{
                return `<td><a href="${{esc(value)}}" target="_blank" rel="noopener noreferrer">Download</a></td>`;
              }}
              return `<td>${{esc(value || '—')}}</td>`;
            }}).join('')}}</tr>`).join('');
            return `<h2 class="section-title">${{esc(tab.label)}}</h2><div class="table-wrap"><table><thead><tr>${{headers}}</tr></thead><tbody>${{body}}</tbody></table></div>`;
          }}
          function renderTab(tabId) {{
            const selected = tabs.find((item) => item.id === tabId) || tabs[0];
            tabRow.innerHTML = tabs.map((tab) => `<button class="tab-button ${{tab.id === selected.id ? 'active' : ''}}" data-tab="${{esc(tab.id)}}">${{esc(tab.label)}}${{tab.count ? ` (${{tab.count}})` : ''}}</button>`).join('');
            tabPanel.innerHTML = selected.isOverview ? renderOverview(selected) : renderTable(selected);
            tabRow.querySelectorAll('[data-tab]').forEach((button) => {{
              button.addEventListener('click', () => renderTab(button.getAttribute('data-tab')));
            }});
          }}
          renderTab('overview');
        </script>
      </body>
    </html>
    """
    return HTMLResponse(html)


@app.get("/api/screamingfrog/audit/file")
def screamingfrog_audit_file(root_domain: str, name: str) -> FileResponse:
    connection = get_connection()
    try:
        export_directory = resolve_screamingfrog_export_directory(connection, root_domain)
    finally:
        connection.close()
    safe_name = Path(name).name
    file_path = (export_directory / safe_name).resolve()
    if export_directory not in file_path.parents or not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="Screaming Frog audit file not found")
    return FileResponse(file_path, filename=file_path.name)


@app.get("/api/analytics")
def analytics(
    search: str | None = None,
    exact_domain: str | None = None,
    countries: list[str] = Query(default=[]),
    tiers: list[str] = Query(default=[]),
    current_platforms: list[str] = Query(default=[]),
    recent_platforms: list[str] = Query(default=[]),
    removed_platforms: list[str] = Query(default=[]),
    verticals: list[str] = Query(default=[]),
    sales_buckets: list[str] = Query(default=[]),
    live_sites_only: bool = False,
    timeline_platforms: list[str] = Query(default=[]),
    timeline_event_types: list[str] = Query(default=[]),
    timeline_date_field: str = "first_seen",
    timeline_seen_from: str | None = None,
    timeline_seen_to: str | None = None,
    cms_migration_from: str | None = None,
    cms_migration_to: str | None = None,
    cms_unchanged_years: int | None = None,
    domain_migration_from: str | None = None,
    domain_migration_to: str | None = None,
    migration_timing_operator: str = "and",
    started_from: str | None = None,
    started_to: str | None = None,
    migration_only: bool = False,
    has_domain_migration: bool = False,
    has_cms_migration: bool = False,
    domain_migration_statuses: list[str] = Query(default=[]),
    domain_confidence_bands: list[str] = Query(default=[]),
    domain_fingerprint_strengths: list[str] = Query(default=[]),
    domain_tld_relationships: list[str] = Query(default=[]),
    cms_migration_statuses: list[str] = Query(default=[]),
    cms_confidence_levels: list[str] = Query(default=[]),
    has_contact: bool = False,
    has_marketing: bool = False,
    has_crm: bool = False,
    has_payments: bool = False,
    marketing_platforms: list[str] = Query(default=[]),
    crm_platforms: list[str] = Query(default=[]),
    payment_platforms: list[str] = Query(default=[]),
    hosting_providers: list[str] = Query(default=[]),
    agencies: list[str] = Query(default=[]),
    ai_tools: list[str] = Query(default=[]),
    compliance_flags: list[str] = Query(default=[]),
    min_social: int | None = None,
    min_revenue: int | None = None,
    min_employees: int | None = None,
    min_sku: int | None = None,
    min_technology_spend: int | None = None,
    selected_only: bool = False,
    has_seranking_analysis: bool = False,
    seranking_analysis_types: list[str] = Query(default=[]),
    seranking_outcome_flags: list[str] = Query(default=[]),
    has_site_status_check: bool = False,
    site_status_categories: list[str] = Query(default=[]),
    has_screamingfrog_audit: bool = False,
    screamingfrog_statuses: list[str] = Query(default=[]),
    screamingfrog_homepage_statuses: list[str] = Query(default=[]),
    screamingfrog_title_flags: list[str] = Query(default=[]),
    screamingfrog_meta_flags: list[str] = Query(default=[]),
    screamingfrog_canonical_flags: list[str] = Query(default=[]),
    has_screamingfrog_internal_errors: bool = False,
    has_screamingfrog_location_pages: bool = False,
    has_screamingfrog_service_pages: bool = False,
) -> dict[str, Any]:
    filters = normalize_lead_filters(locals())
    connection = get_connection()
    try:
        _where, _params, rows = fetch_filtered_rows(connection, filters=filters)
        migrations = connection.execute(
            """
            select root_domain, old_platform, new_platform, migration_status
            from cms_migration_pairs_v2
            where migration_status in ('confirmed', 'possible', 'historic')
            """
        ).fetchall()
        return build_analytics_payload(rows, migrations)
    finally:
        connection.close()


@app.get("/api/timeline/cohort")
def timeline_cohort(
    search: str | None = None,
    exact_domain: str | None = None,
    countries: list[str] = Query(default=[]),
    tiers: list[str] = Query(default=[]),
    current_platforms: list[str] = Query(default=[]),
    recent_platforms: list[str] = Query(default=[]),
    removed_platforms: list[str] = Query(default=[]),
    verticals: list[str] = Query(default=[]),
    sales_buckets: list[str] = Query(default=[]),
    live_sites_only: bool = False,
    timeline_platforms: list[str] = Query(default=[]),
    timeline_event_types: list[str] = Query(default=[]),
    timeline_date_field: str = "first_seen",
    timeline_seen_from: str | None = None,
    timeline_seen_to: str | None = None,
    cms_migration_from: str | None = None,
    cms_migration_to: str | None = None,
    cms_unchanged_years: int | None = None,
    domain_migration_from: str | None = None,
    domain_migration_to: str | None = None,
    migration_timing_operator: str = "and",
    started_from: str | None = None,
    started_to: str | None = None,
    granularity: str = "month",
    migration_only: bool = False,
    has_domain_migration: bool = False,
    has_cms_migration: bool = False,
    domain_migration_statuses: list[str] = Query(default=[]),
    domain_confidence_bands: list[str] = Query(default=[]),
    domain_fingerprint_strengths: list[str] = Query(default=[]),
    domain_tld_relationships: list[str] = Query(default=[]),
    cms_migration_statuses: list[str] = Query(default=[]),
    cms_confidence_levels: list[str] = Query(default=[]),
    has_contact: bool = False,
    has_marketing: bool = False,
    has_crm: bool = False,
    has_payments: bool = False,
    marketing_platforms: list[str] = Query(default=[]),
    crm_platforms: list[str] = Query(default=[]),
    payment_platforms: list[str] = Query(default=[]),
    hosting_providers: list[str] = Query(default=[]),
    agencies: list[str] = Query(default=[]),
    ai_tools: list[str] = Query(default=[]),
    compliance_flags: list[str] = Query(default=[]),
    min_social: int | None = None,
    min_revenue: int | None = None,
    min_employees: int | None = None,
    min_sku: int | None = None,
    min_technology_spend: int | None = None,
    selected_only: bool = False,
    has_seranking_analysis: bool = False,
    seranking_analysis_types: list[str] = Query(default=[]),
    seranking_outcome_flags: list[str] = Query(default=[]),
    has_site_status_check: bool = False,
    site_status_categories: list[str] = Query(default=[]),
    has_screamingfrog_audit: bool = False,
    screamingfrog_statuses: list[str] = Query(default=[]),
    screamingfrog_homepage_statuses: list[str] = Query(default=[]),
    screamingfrog_title_flags: list[str] = Query(default=[]),
    screamingfrog_meta_flags: list[str] = Query(default=[]),
    screamingfrog_canonical_flags: list[str] = Query(default=[]),
    has_screamingfrog_internal_errors: bool = False,
    has_screamingfrog_location_pages: bool = False,
    has_screamingfrog_service_pages: bool = False,
) -> dict[str, Any]:
    filters = normalize_lead_filters(locals())
    if granularity not in {"week", "month", "quarter"}:
        raise HTTPException(status_code=400, detail="Unsupported timeline granularity")
    if not filters["timeline_platforms"]:
        return build_timeline_cohort_payload([], granularity, filters["timeline_date_field"])

    connection = get_connection()
    try:
        migration_join_sql, _migration_select_sql = build_migration_join_and_select_sql()
        seranking_join_sql, _seranking_select_sql = build_seranking_join_and_select_sql()
        site_status_join_sql, _site_status_select_sql = build_site_status_join_and_select_sql()
        screamingfrog_join_sql, _screamingfrog_select_sql = build_screamingfrog_join_and_select_sql()
        where, params = build_lead_filters(filters)
        base_join_sql = filter_base_joins(filters)
        cohort_clause = build_timeline_clause(
            "tt",
            filters["timeline_platforms"],
            filters["timeline_event_types"],
            filters["timeline_date_field"],
            filters["timeline_seen_from"],
            filters["timeline_seen_to"],
            params,
            "cohort",
        )
        timeline_value_column = "tt.last_found" if filters["timeline_date_field"] == "last_seen" else "tt.first_detected"
        rows = connection.execute(
            f"""
            select tt.root_domain, tt.platform, {timeline_value_column} as date_value
            from technology_timelines tt
            join leads on leads.root_domain = tt.root_domain
            {base_join_sql}
            {migration_join_sql}
            {seranking_join_sql}
            {site_status_join_sql}
            {screamingfrog_join_sql}
            where {where} and {cohort_clause}
            order by date_value asc, tt.platform asc, tt.root_domain asc
            """,
            params,
        ).fetchall()
        return build_timeline_cohort_payload(rows, granularity, filters["timeline_date_field"])
    finally:
        connection.close()


@app.get("/api/leads")
def list_leads(
    search: str | None = None,
    exact_domain: str | None = None,
    countries: list[str] = Query(default=[]),
    tiers: list[str] = Query(default=[]),
    current_platforms: list[str] = Query(default=[]),
    recent_platforms: list[str] = Query(default=[]),
    removed_platforms: list[str] = Query(default=[]),
    verticals: list[str] = Query(default=[]),
    sales_buckets: list[str] = Query(default=[]),
    live_sites_only: bool = False,
    timeline_platforms: list[str] = Query(default=[]),
    timeline_event_types: list[str] = Query(default=[]),
    timeline_date_field: str = "first_seen",
    timeline_seen_from: str | None = None,
    timeline_seen_to: str | None = None,
    cms_migration_from: str | None = None,
    cms_migration_to: str | None = None,
    cms_unchanged_years: int | None = None,
    domain_migration_from: str | None = None,
    domain_migration_to: str | None = None,
    migration_timing_operator: str = "and",
    started_from: str | None = None,
    started_to: str | None = None,
    migration_only: bool = False,
    has_domain_migration: bool = False,
    has_cms_migration: bool = False,
    domain_migration_statuses: list[str] = Query(default=[]),
    domain_confidence_bands: list[str] = Query(default=[]),
    domain_fingerprint_strengths: list[str] = Query(default=[]),
    domain_tld_relationships: list[str] = Query(default=[]),
    cms_migration_statuses: list[str] = Query(default=[]),
    cms_confidence_levels: list[str] = Query(default=[]),
    has_contact: bool = False,
    has_marketing: bool = False,
    has_crm: bool = False,
    has_payments: bool = False,
    marketing_platforms: list[str] = Query(default=[]),
    crm_platforms: list[str] = Query(default=[]),
    payment_platforms: list[str] = Query(default=[]),
    hosting_providers: list[str] = Query(default=[]),
    agencies: list[str] = Query(default=[]),
    ai_tools: list[str] = Query(default=[]),
    compliance_flags: list[str] = Query(default=[]),
    min_social: int | None = None,
    min_revenue: int | None = None,
    min_employees: int | None = None,
    min_sku: int | None = None,
    min_technology_spend: int | None = None,
    selected_only: bool = False,
    has_seranking_analysis: bool = False,
    seranking_analysis_types: list[str] = Query(default=[]),
    seranking_outcome_flags: list[str] = Query(default=[]),
    has_site_status_check: bool = False,
    site_status_categories: list[str] = Query(default=[]),
    has_screamingfrog_audit: bool = False,
    screamingfrog_statuses: list[str] = Query(default=[]),
    screamingfrog_homepage_statuses: list[str] = Query(default=[]),
    screamingfrog_title_flags: list[str] = Query(default=[]),
    screamingfrog_meta_flags: list[str] = Query(default=[]),
    screamingfrog_canonical_flags: list[str] = Query(default=[]),
    has_screamingfrog_internal_errors: bool = False,
    has_screamingfrog_location_pages: bool = False,
    has_screamingfrog_service_pages: bool = False,
    sort_by: str = DEFAULT_SORT_BY,
    sort_direction: str = DEFAULT_SORT_DIRECTION,
    page: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> dict[str, Any]:
    safe_page = max(page, 1)
    safe_page_size = min(max(page_size, 1), MAX_PAGE_SIZE)
    filters = normalize_lead_filters(locals())
    migration_join_sql, migration_select_sql = build_migration_join_and_select_sql()
    seranking_join_sql, seranking_select_sql = build_seranking_join_and_select_sql()
    site_status_join_sql, site_status_select_sql = build_site_status_join_and_select_sql()
    screamingfrog_join_sql, screamingfrog_select_sql = build_screamingfrog_join_and_select_sql()
    where, params = build_lead_filters(filters, apply_timeline_match=not bool(filters["timeline_platforms"]))
    base_join_sql = filter_base_joins(filters)
    timeline_join_sql, matched_select_sql = build_timeline_join_and_select_sql(
        filters["timeline_platforms"],
        filters["timeline_event_types"],
        filters["timeline_date_field"],
        filters["timeline_seen_from"],
        filters["timeline_seen_to"],
        params,
    )
    count_join_sql = filter_count_joins(filters)
    order_search = search or exact_domain
    if normalize_domain_search(order_search):
        params["search_exact_domain"] = normalize_domain_search(order_search)
    params["limit"] = safe_page_size
    params["offset"] = (safe_page - 1) * safe_page_size
    order_clause = build_order_clause(sort_by, sort_direction, order_search)

    connection = get_connection()
    try:
        total = connection.execute(
            f"""
            select count(*)
            from leads
            {count_join_sql}
            {timeline_join_sql}
            where {where}
            """,
            params,
        ).fetchone()[0]
        rows = connection.execute(
            f"""
            select leads.*, {migration_select_sql}, {seranking_select_sql}, {site_status_select_sql}, {screamingfrog_select_sql}, {matched_select_sql}
            from leads
            {base_join_sql}
            {migration_join_sql}
            {seranking_join_sql}
            {site_status_join_sql}
            {screamingfrog_join_sql}
            {timeline_join_sql}
            where {where}
            order by {order_clause}
            limit :limit offset :offset
            """,
            params,
        ).fetchall()

        tray_rows = {
            row["root_domain"]
            for row in connection.execute("select root_domain from state.export_tray_items")
        }

        items = []
        for row in rows:
            item = lead_row_to_item(row)
            item["is_selected"] = item["root_domain"] in tray_rows
            items.append(item)

        return {
            "items": items,
            "total": total,
            "page": safe_page,
            "pageSize": safe_page_size,
            "pages": (total + safe_page_size - 1) // safe_page_size,
            "sortBy": normalize_sort(sort_by, sort_direction)[0],
            "sortDirection": normalize_sort(sort_by, sort_direction)[1],
        }
    finally:
        connection.close()


@app.get("/api/leads/export")
def export_leads(
    search: str | None = None,
    exact_domain: str | None = None,
    countries: list[str] = Query(default=[]),
    tiers: list[str] = Query(default=[]),
    current_platforms: list[str] = Query(default=[]),
    recent_platforms: list[str] = Query(default=[]),
    removed_platforms: list[str] = Query(default=[]),
    verticals: list[str] = Query(default=[]),
    sales_buckets: list[str] = Query(default=[]),
    live_sites_only: bool = False,
    timeline_platforms: list[str] = Query(default=[]),
    timeline_event_types: list[str] = Query(default=[]),
    timeline_date_field: str = "first_seen",
    timeline_seen_from: str | None = None,
    timeline_seen_to: str | None = None,
    cms_migration_from: str | None = None,
    cms_migration_to: str | None = None,
    cms_unchanged_years: int | None = None,
    domain_migration_from: str | None = None,
    domain_migration_to: str | None = None,
    migration_timing_operator: str = "and",
    started_from: str | None = None,
    started_to: str | None = None,
    migration_only: bool = False,
    has_domain_migration: bool = False,
    has_cms_migration: bool = False,
    domain_migration_statuses: list[str] = Query(default=[]),
    domain_confidence_bands: list[str] = Query(default=[]),
    domain_fingerprint_strengths: list[str] = Query(default=[]),
    domain_tld_relationships: list[str] = Query(default=[]),
    cms_migration_statuses: list[str] = Query(default=[]),
    cms_confidence_levels: list[str] = Query(default=[]),
    has_contact: bool = False,
    has_marketing: bool = False,
    has_crm: bool = False,
    has_payments: bool = False,
    marketing_platforms: list[str] = Query(default=[]),
    crm_platforms: list[str] = Query(default=[]),
    payment_platforms: list[str] = Query(default=[]),
    hosting_providers: list[str] = Query(default=[]),
    agencies: list[str] = Query(default=[]),
    ai_tools: list[str] = Query(default=[]),
    compliance_flags: list[str] = Query(default=[]),
    min_social: int | None = None,
    min_revenue: int | None = None,
    min_employees: int | None = None,
    min_sku: int | None = None,
    min_technology_spend: int | None = None,
    selected_only: bool = False,
    has_seranking_analysis: bool = False,
    seranking_analysis_types: list[str] = Query(default=[]),
    seranking_outcome_flags: list[str] = Query(default=[]),
    has_site_status_check: bool = False,
    site_status_categories: list[str] = Query(default=[]),
    has_screamingfrog_audit: bool = False,
    screamingfrog_statuses: list[str] = Query(default=[]),
    screamingfrog_homepage_statuses: list[str] = Query(default=[]),
    screamingfrog_title_flags: list[str] = Query(default=[]),
    screamingfrog_meta_flags: list[str] = Query(default=[]),
    screamingfrog_canonical_flags: list[str] = Query(default=[]),
    has_screamingfrog_internal_errors: bool = False,
    has_screamingfrog_location_pages: bool = False,
    has_screamingfrog_service_pages: bool = False,
    sort_by: str = DEFAULT_SORT_BY,
    sort_direction: str = DEFAULT_SORT_DIRECTION,
) -> StreamingResponse:
    filters = normalize_lead_filters(locals())
    migration_join_sql, migration_select_sql = build_migration_join_and_select_sql()
    seranking_join_sql, seranking_select_sql = build_seranking_join_and_select_sql()
    site_status_join_sql, site_status_select_sql = build_site_status_join_and_select_sql()
    screamingfrog_join_sql, screamingfrog_select_sql = build_screamingfrog_join_and_select_sql()
    where, params = build_lead_filters(filters, apply_timeline_match=not bool(filters["timeline_platforms"]))
    base_join_sql = filter_base_joins(filters)
    timeline_join_sql, matched_select_sql = build_timeline_join_and_select_sql(
        filters["timeline_platforms"],
        filters["timeline_event_types"],
        filters["timeline_date_field"],
        filters["timeline_seen_from"],
        filters["timeline_seen_to"],
        params,
    )
    order_search = search or exact_domain
    if normalize_domain_search(order_search):
        params["search_exact_domain"] = normalize_domain_search(order_search)
    order_clause = build_order_clause(sort_by, sort_direction, order_search)
    connection = get_connection()
    try:
        rows = connection.execute(
            f"""
            select leads.*, {migration_select_sql}, {seranking_select_sql}, {site_status_select_sql}, {screamingfrog_select_sql}, {matched_select_sql}
            from leads
            {base_join_sql}
            {migration_join_sql}
            {seranking_join_sql}
            {site_status_join_sql}
            {screamingfrog_join_sql}
            {timeline_join_sql}
            where {where}
            order by {order_clause}
            """,
            params,
        ).fetchall()
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        if rows:
            writer.writerow(rows[0].keys())
            for row in rows:
                writer.writerow([row[key] for key in row.keys()])
        buffer.seek(0)
        filename = "selected-leads.csv" if selected_only else "lead-export.csv"
        return StreamingResponse(
            iter([buffer.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )
    finally:
        connection.close()


@app.get("/api/leads/{root_domain:path}")
def lead_detail(root_domain: str) -> dict[str, Any]:
    connection = get_connection()
    try:
        migration_join_sql, migration_select_sql = build_migration_join_and_select_sql()
        seranking_join_sql, seranking_select_sql = build_seranking_join_and_select_sql()
        site_status_join_sql, site_status_select_sql = build_site_status_join_and_select_sql()
        screamingfrog_join_sql, screamingfrog_select_sql = build_screamingfrog_join_and_select_sql()
        lead = connection.execute(
            f"""
            select leads.*, {migration_select_sql}, {seranking_select_sql}, {site_status_select_sql}, {screamingfrog_select_sql}
            from leads
            {migration_join_sql}
            {seranking_join_sql}
            {site_status_join_sql}
            {screamingfrog_join_sql}
            where leads.root_domain = ?
            """,
            (root_domain,),
        ).fetchone()
        if lead is None:
            raise HTTPException(status_code=404, detail="Lead not found")

        events = connection.execute(
            """
            select *
            from platform_events
            where root_domain = ?
            order by
                case event_type
                    when 'current_detected' then 1
                    when 'recently_added' then 2
                    when 'no_longer_detected' then 3
                    else 4
                end,
                last_found desc,
                first_detected desc
            """,
            (root_domain,),
        ).fetchall()
        migrations = connection.execute(
            """
            select *
            from cms_migration_pairs_v2
            where root_domain = ?
            order by
                case migration_status
                    when 'confirmed' then 1
                    when 'possible' then 2
                    when 'overlap' then 3
                    when 'historic' then 4
                    when 'removed_only' then 5
                    else 6
                end,
                case lower(coalesce(confidence_level, ''))
                    when 'high' then 1
                    when 'medium' then 2
                    when 'low' then 3
                    else 4
                end,
                case
                    when gap_days is null or gap_days = '' then 999999
                    else abs(cast(gap_days as integer))
                end asc,
                coalesce(first_new_detected, '') desc
            """,
            (root_domain,),
        ).fetchall()
        timeline_rows = connection.execute(
            """
            select *
            from technology_timelines
            where root_domain = ?
            order by first_detected asc, last_found asc, platform asc
            """,
            (root_domain,),
        ).fetchall()

        selected = (
            connection.execute(
                "select 1 from state.export_tray_items where root_domain = ?",
                (root_domain,),
            ).fetchone()
            is not None
        )

        domain_best_match = connection.execute(
            """
            select *
            from domain_migration_best_match_ui
            where current_domain = ?
            """,
            (root_domain,),
        ).fetchone()
        domain_candidate_shortlist = connection.execute(
            """
            select *
            from domain_migration_candidates_enriched
            where current_domain = ?
            order by
                case
                    when coalesce(fingerprint_strength, '') in ('Strong', 'Moderate') then 6
                    when domain_tld_relationship(current_domain, old_domain) = 'same_tld'
                         and cast(coalesce(enhanced_confidence_score, 0) as integer) >= 60 then 5
                    when coalesce(shared_signal_flags, '') != ''
                         or coalesce(shared_high_signal_technologies, '') != '' then 4
                    when coalesce(fingerprint_strength, '') = 'Weak' then 3
                    when cast(coalesce(enhanced_confidence_score, 0) as integer) >= 80 then 2
                    else 1
                end desc,
                case domain_tld_relationship(current_domain, old_domain)
                    when 'same_tld' then 2
                    when 'cross_tld' then 1
                    else 0
                end desc,
                cast(coalesce(enhanced_confidence_score, 0) as integer) desc,
                cast(coalesce(fingerprint_score, 0) as integer) desc,
                cast(coalesce(redirect_duration_days, 0) as integer) desc,
                lower(old_domain) asc
            limit 5
            """,
            (root_domain,),
        ).fetchall()

        lead_item = lead_row_to_item(lead)
        lead_item["emails"] = split_pipe(lead["emails"])
        lead_item["telephones"] = split_pipe(lead["telephones"])
        lead_item["people"] = split_pipe(lead["people"])
        lead_item["verified_profiles"] = split_pipe(lead["verified_profiles"])

        domain_best_payload = None
        if domain_best_match is not None:
            best_row = dict(domain_best_match)
            domain_best_payload = {
                **best_row,
                "shared_signal_flags": split_pipe(best_row.get("domain_shared_signals")),
                "shared_high_signal_technologies": split_pipe(best_row.get("domain_shared_technologies")),
                "domain_migration_warning_flags": split_pipe(best_row.get("domain_migration_warning_flags")),
                "domain_migration_evidence_flags": split_pipe(best_row.get("domain_migration_evidence_flags")),
            }

        domain_candidate_payload = []
        for row in domain_candidate_shortlist:
            item = dict(row)
            item["domain_tld_relationship"] = compute_domain_tld_relationship(
                item.get("current_domain"),
                item.get("old_domain"),
            )
            item["shared_signal_flags"] = split_pipe(item.get("shared_signal_flags"))
            item["shared_high_signal_technologies"] = split_pipe(item.get("shared_high_signal_technologies"))
            domain_candidate_payload.append(item)

        cms_candidate_pairs = []
        for row in migrations:
            item = dict(row)
            item["likely_migration_date"] = item.get("likely_migration_date") or midpoint_iso_date(
                item.get("last_old_found"),
                item.get("first_new_detected"),
            )
            item["warning_flags"] = split_pipe(item.get("warning_flags"))
            item["evidence_flags"] = split_pipe(item.get("evidence_flags"))
            cms_candidate_pairs.append(item)

        best_cms_payload = None
        if lead_item["cms_migration_status"] != "none":
            best_cms_payload = {
                "old_platform": lead_item["cms_old_platform"],
                "new_platform": lead_item["cms_new_platform"],
                "migration_status": lead_item["cms_migration_status"],
                "confidence_level": lead_item["cms_migration_confidence"],
                "migration_reason": lead_item["cms_migration_reason"],
                "first_new_detected": lead_item["cms_first_new_detected"],
                "last_old_found": lead_item["cms_last_old_found"],
                "likely_migration_date": lead_item["cms_migration_likely_date"],
                "gap_days": lead_item["cms_migration_gap_days"],
                "warning_flags": lead_item["cms_migration_warning_flags"],
                "evidence_flags": lead_item["cms_migration_evidence_flags"],
            }

        quality_notes: list[str] = []
        if lead_item["cms_migration_status"] in {"possible", "overlap", "removed_only"}:
            quality_notes.append("CMS migration needs review")
        if lead_item["domain_migration_status"] in {"probable", "network", "weak"}:
            quality_notes.append("Previous-domain match needs review")
        if "conflicting_platform_snapshot" in lead_item["integrity_flags"]:
            quality_notes.append("BuiltWith removal rows contain conflicting snapshot platform metadata")

        seranking_row = connection.execute(
            """
            select *
            from state.seranking_analysis_snapshots
            where root_domain = ?
            order by captured_at desc
            """,
            (root_domain,),
        ).fetchone()
        site_status_row = connection.execute(
            """
            select *
            from state.site_status_snapshots
            where root_domain = ?
            """,
            (root_domain,),
        ).fetchone()
        screamingfrog_row = connection.execute(
            """
            select *
            from state.screamingfrog_audit_snapshots
            where root_domain = ?
            """,
            (root_domain,),
        ).fetchone()

        return {
            "lead": lead_item,
            "selected": selected,
            "events": [dict(row) for row in events],
            "migrations": cms_candidate_pairs,
            "migrationIntelligence": {
                "summary": {
                    "hasDomainMigration": domain_best_payload is not None,
                    "hasCmsMigration": bool(best_cms_payload and best_cms_payload["new_platform"]),
                    "domainCandidateCount": (
                        int(domain_best_payload.get("domain_migration_candidate_count") or len(domain_candidate_payload))
                        if domain_best_payload
                        else 0
                    ),
                    "cmsCandidateCount": len(cms_candidate_pairs),
                },
                "domainMigration": {
                    "bestMatch": domain_best_payload,
                    "candidateShortlist": domain_candidate_payload,
                },
                "cmsMigration": {
                    "bestPair": best_cms_payload,
                    "candidatePairs": cms_candidate_pairs,
                },
            },
            "domainMigrationV2": {
                "bestMatch": domain_best_payload,
                "candidateShortlist": domain_candidate_payload,
            },
            "cmsMigrationV2": {
                "bestPair": best_cms_payload,
                "candidatePairs": cms_candidate_pairs,
            },
            "data_quality": {
                "leadFlags": lead_item["integrity_flags"],
                "cmsWarnings": lead_item["cms_migration_warning_flags"],
                "domainWarnings": lead_item["domain_migration_warning_flags"],
                "notes": quality_notes,
            },
            "timelineRows": [
                {
                    **dict(row),
                    "event_types": split_pipe(row["event_types"]),
                }
                for row in timeline_rows
            ],
            "exportReady": {
                "root_domain": lead["root_domain"],
                "company": lead["company"],
                "country": lead["country"],
                "emails": split_pipe(lead["emails"]),
                "telephones": split_pipe(lead["telephones"]),
                "people": split_pipe(lead["people"]),
                "bucket_reasons": [reason.strip() for reason in lead["bucket_reasons"].split("||") if reason.strip()],
            },
            "seRankingAnalysis": (
                {
                    **dict(seranking_row),
                    "outcome_flags": split_pipe(seranking_row["outcome_flags"]),
                }
                if seranking_row
                else None
            ),
            "siteStatusCheck": ({**dict(site_status_row)} if site_status_row else None),
            "screamingFrogAudit": (
                {
                    **dict(screamingfrog_row),
                    "sitemap_url": unescape(str(screamingfrog_row["sitemap_url"] or "")),
                }
                if screamingfrog_row
                else None
            ),
        }
    finally:
        connection.close()
