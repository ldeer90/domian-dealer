from __future__ import annotations

import csv
import io
import json
import os
import re
import sqlite3
import threading
import uuid
from collections import Counter, defaultdict
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from backend.builtin_presets import BUILTIN_PRESETS, DEFAULT_VISIBLE_COLUMNS


ROOT = Path("/Users/laurencedeer/Desktop/BuiltWith")
DATA_DB_PATH = ROOT / "processed" / "builtwith.db"
SUMMARY_PATH = ROOT / "processed" / "summary.json"
FILTER_OPTIONS_PATH = ROOT / "processed" / "filter_options.json"
STATE_DB_PATH = ROOT / "processed" / "lead_console_state.db"

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
SERANKING_ANALYSIS_TYPES = ["cms_migration", "domain_migration"]
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
SERANKING_API_BASE = "https://api.seranking.com/v1"
SERANKING_API_KEY = os.getenv("SERANKING_API_KEY", "")
SERANKING_SOURCE_MAP = {"AU": "au", "NZ": "nz", "SG": "sg"}

STATE_DB_INIT_LOCK = threading.Lock()
STATE_DB_INITIALIZED = False

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
    "se_ranking_checked_at": "coalesce(se_ranking_checked_at, '')",
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


class SeRankingRefreshPayload(BaseModel):
    analysis_type: str = Field(pattern="^(cms_migration|domain_migration)$")


app = FastAPI(title="BuiltWith Lead Console", version="0.2.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


def split_pipe(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split("|") if item.strip()]


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
    return "domain_migration" if (value or "").lower() == "domain_migration" else "cms_migration"


def normalize_seranking_outcome_flags(values: list[str]) -> list[str]:
    return [value for value in values if value in SERANKING_OUTCOME_FLAGS]


def parse_iso_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).date()
    except ValueError:
        return None


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


def within_last_twelve_months(value: date | None) -> bool:
    if value is None:
        return False
    earliest = shift_months(last_full_month_start(), -11)
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


def ensure_state_db() -> None:
    global STATE_DB_INITIALIZED
    if STATE_DB_INITIALIZED:
        return

    with STATE_DB_INIT_LOCK:
        if STATE_DB_INITIALIZED:
            return

        STATE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(STATE_DB_PATH, timeout=30)
        try:
            connection.execute("pragma journal_mode = wal")
            connection.execute("pragma synchronous = normal")
            connection.execute("pragma busy_timeout = 30000")
            connection.execute(
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
            )
            connection.execute(
                """
                create table if not exists export_tray_items (
                    root_domain text primary key,
                    added_at text not null
                )
                """
            )
            connection.execute(
                """
                create table if not exists seranking_analysis_snapshots (
                    root_domain text not null,
                    analysis_type text not null,
                    regional_source text not null,
                    migration_likely_date text not null,
                    baseline_month text not null,
                    comparison_month text not null,
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

            for preset in BUILTIN_PRESETS:
                created_at = now_iso()
                connection.execute(
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
                )
            connection.commit()
            STATE_DB_INITIALIZED = True
        finally:
            connection.close()


def get_connection() -> sqlite3.Connection:
    ensure_state_db()
    connection = sqlite3.connect(DATA_DB_PATH, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.create_function("domain_tld_relationship", 2, compute_domain_tld_relationship)
    connection.execute("pragma busy_timeout = 30000")
    connection.execute(f"attach database '{STATE_DB_PATH.as_posix()}' as state")
    connection.execute("pragma state.busy_timeout = 30000")
    return connection


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


def build_lead_filters(
    search: str | None,
    exact_domain: str | None,
    countries: list[str],
    tiers: list[str],
    current_platforms: list[str],
    recent_platforms: list[str],
    removed_platforms: list[str],
    verticals: list[str],
    sales_buckets: list[str],
    timeline_platforms: list[str],
    timeline_event_types: list[str],
    timeline_date_field: str,
    timeline_seen_from: str | None,
    timeline_seen_to: str | None,
    cms_migration_from: str | None,
    cms_migration_to: str | None,
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
    selected_only: bool,
    has_seranking_analysis: bool,
    seranking_analysis_types: list[str],
    seranking_outcome_flags: list[str],
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
    return {
        **data,
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
        "sales_buckets": split_pipe(data.get("sales_buckets")),
        "cms_migration_warning_flags": split_pipe(data.get("cms_migration_warning_flags")),
        "cms_migration_evidence_flags": split_pipe(data.get("cms_migration_evidence_flags")),
        "se_ranking_outcome_flags": split_pipe(data.get("se_ranking_outcome_flags")),
        "bucket_reasons_list": [reason.strip() for reason in (data.get("bucket_reasons") or "").split("||") if reason.strip()],
        "bucket_count": len(split_pipe(data.get("sales_buckets"))),
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
        coalesce(se_ranking.regional_source, '') as se_ranking_market,
        coalesce(se_ranking.migration_likely_date, '') as se_ranking_migration_date,
        coalesce(se_ranking.baseline_month, '') as se_ranking_baseline_month,
        coalesce(se_ranking.comparison_month, '') as se_ranking_comparison_month,
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
        elif not within_last_twelve_months(migration_date_value):
            eligible = False
            eligibility_reason = "Migration is older than 12 months"
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
                "comparison_month": month_key(last_full_month_start()),
                "eligible": eligible,
                "eligibility_reason": eligibility_reason,
                "already_analyzed": (row["root_domain"], normalized_type) in existing,
            }
        )
    candidates.sort(key=lambda item: item["root_domain"])
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
            root_domain, analysis_type, regional_source, migration_likely_date, baseline_month, comparison_month,
            traffic_before, traffic_last_month, traffic_delta_absolute, traffic_delta_percent,
            keywords_before, keywords_last_month, keywords_delta_absolute, keywords_delta_percent,
            price_before, price_last_month, price_delta_absolute, price_delta_percent,
            outcome_flags, captured_at, status, error_message
        ) values (
            :root_domain, :analysis_type, :regional_source, :migration_likely_date, :baseline_month, :comparison_month,
            :traffic_before, :traffic_last_month, :traffic_delta_absolute, :traffic_delta_percent,
            :keywords_before, :keywords_last_month, :keywords_delta_absolute, :keywords_delta_percent,
            :price_before, :price_last_month, :price_delta_absolute, :price_delta_percent,
            :outcome_flags, :captured_at, :status, :error_message
        )
        on conflict(root_domain, analysis_type) do update set
            regional_source=excluded.regional_source,
            migration_likely_date=excluded.migration_likely_date,
            baseline_month=excluded.baseline_month,
            comparison_month=excluded.comparison_month,
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
) -> dict[str, Any]:
    normalized_type = normalize_seranking_analysis_type(analysis_type)
    candidates = selected_tray_analysis_candidates(connection, normalized_type)
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


def build_filter_payload_from_row(row: sqlite3.Row) -> dict[str, Any]:
    filters = decode_json_text(row["filters_json"]) or {}
    sort = decode_json_text(row["sort_json"]) or {}
    return {
        "id": row["id"],
        "name": row["name"],
        "isBuiltin": bool(row["is_builtin"]),
        "filters": filters.get("query", filters),
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
        from state.export_tray_items tray
        join leads on leads.root_domain = tray.root_domain
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
    search: str | None,
    exact_domain: str | None,
    countries: list[str],
    tiers: list[str],
    current_platforms: list[str],
    recent_platforms: list[str],
    removed_platforms: list[str],
    verticals: list[str],
    sales_buckets: list[str],
    timeline_platforms: list[str],
    timeline_event_types: list[str],
    timeline_date_field: str,
    timeline_seen_from: str | None,
    timeline_seen_to: str | None,
    cms_migration_from: str | None,
    cms_migration_to: str | None,
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
    selected_only: bool,
    has_seranking_analysis: bool,
    seranking_analysis_types: list[str],
    seranking_outcome_flags: list[str],
) -> tuple[str, dict[str, Any], list[sqlite3.Row]]:
    migration_join_sql, migration_select_sql = build_migration_join_and_select_sql()
    seranking_join_sql, _seranking_select_sql = build_seranking_join_and_select_sql()
    where, params = build_lead_filters(
        search,
        exact_domain,
        countries,
        tiers,
        current_platforms,
        recent_platforms,
        removed_platforms,
        verticals,
        sales_buckets,
        timeline_platforms,
        timeline_event_types,
        timeline_date_field,
        timeline_seen_from,
        timeline_seen_to,
        cms_migration_from,
        cms_migration_to,
        domain_migration_from,
        domain_migration_to,
        migration_timing_operator,
        migration_only,
        has_domain_migration,
        has_cms_migration,
        domain_migration_statuses,
        domain_confidence_bands,
        domain_fingerprint_strengths,
        domain_tld_relationships,
        cms_migration_statuses,
        cms_confidence_levels,
        has_contact,
        has_marketing,
        has_crm,
        has_payments,
        selected_only,
        has_seranking_analysis,
        seranking_analysis_types,
        seranking_outcome_flags,
    )
    rows = connection.execute(
        f"""
        select leads.*, {migration_select_sql}
        from leads
        {migration_join_sql}
        {seranking_join_sql}
        where {where}
        """,
        params,
    ).fetchall()
    return where, params, rows


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/summary")
def summary() -> dict[str, Any]:
    return json.loads(SUMMARY_PATH.read_text(encoding="utf-8"))


@app.get("/api/filter-options")
def filter_options() -> dict[str, Any]:
    payload = json.loads(FILTER_OPTIONS_PATH.read_text(encoding="utf-8"))
    payload["domainMigrationStatuses"] = DOMAIN_MIGRATION_STATUSES
    payload["domainConfidenceBands"] = DOMAIN_CONFIDENCE_BANDS
    payload["domainFingerprintStrengths"] = DOMAIN_FINGERPRINT_STRENGTHS
    payload["domainTldRelationships"] = DOMAIN_TLD_RELATIONSHIPS
    payload["cmsMigrationStatuses"] = CMS_MIGRATION_STATUSES
    payload["cmsConfidenceLevels"] = CMS_CONFIDENCE_LEVELS
    payload["seRankingAnalysisTypes"] = SERANKING_ANALYSIS_TYPES
    payload["seRankingOutcomeFlags"] = SERANKING_OUTCOME_FLAGS
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
            "query": payload.filters,
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
            "query": payload.filters,
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
    connection = get_connection()
    try:
        return build_export_tray_payload(connection)
    finally:
        connection.close()


@app.post("/api/export-tray/items")
def add_export_tray_items(payload: TrayMutationPayload) -> dict[str, Any]:
    connection = get_connection()
    try:
        created_at = now_iso()
        for root_domain in payload.root_domains:
            connection.execute(
                """
                insert into state.export_tray_items (root_domain, added_at)
                values (?, ?)
                on conflict(root_domain) do update set added_at = excluded.added_at
                """,
                (root_domain, created_at),
            )
        connection.commit()
        return build_export_tray_payload(connection)
    finally:
        connection.close()


@app.delete("/api/export-tray/items/{root_domain:path}")
def delete_export_tray_item(root_domain: str) -> dict[str, Any]:
    connection = get_connection()
    try:
        connection.execute("delete from state.export_tray_items where root_domain = ?", (root_domain,))
        connection.commit()
        return build_export_tray_payload(connection)
    finally:
        connection.close()


@app.post("/api/export-tray/clear")
def clear_export_tray() -> dict[str, Any]:
    connection = get_connection()
    try:
        connection.execute("delete from state.export_tray_items")
        connection.commit()
        return build_export_tray_payload(connection)
    finally:
        connection.close()


@app.get("/api/seranking/summary")
def seranking_summary(analysis_type: str = "cms_migration") -> dict[str, Any]:
    connection = get_connection()
    try:
        normalized_type = normalize_seranking_analysis_type(analysis_type)
        candidates = selected_tray_analysis_candidates(connection, normalized_type)
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
        candidates = selected_tray_analysis_candidates(connection, normalized_type)
        summary = summarize_seranking_candidates(candidates, skip_existing=True)
        if not payload.confirm:
            return {"analysisType": normalized_type, "summary": summary, "results": []}
        result = run_seranking_analysis(connection, normalized_type, refresh_existing=False)
        return {"analysisType": normalized_type, **result}
    finally:
        connection.close()


@app.post("/api/seranking/refresh")
def seranking_refresh(payload: SeRankingRefreshPayload) -> dict[str, Any]:
    connection = get_connection()
    try:
        normalized_type = normalize_seranking_analysis_type(payload.analysis_type)
        result = run_seranking_analysis(connection, normalized_type, refresh_existing=True)
        return {"analysisType": normalized_type, **result}
    finally:
        connection.close()


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
    timeline_platforms: list[str] = Query(default=[]),
    timeline_event_types: list[str] = Query(default=[]),
    timeline_date_field: str = "first_seen",
    timeline_seen_from: str | None = None,
    timeline_seen_to: str | None = None,
    cms_migration_from: str | None = None,
    cms_migration_to: str | None = None,
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
    selected_only: bool = False,
    has_seranking_analysis: bool = False,
    seranking_analysis_types: list[str] = Query(default=[]),
    seranking_outcome_flags: list[str] = Query(default=[]),
) -> dict[str, Any]:
    effective_timeline_seen_from = timeline_seen_from or started_from
    effective_timeline_seen_to = timeline_seen_to or started_to
    effective_timeline_date_field = normalize_timeline_date_field(timeline_date_field)
    effective_migration_timing_operator = normalize_migration_timing_operator(migration_timing_operator)
    connection = get_connection()
    try:
        _where, _params, rows = fetch_filtered_rows(
            connection,
            search=search,
            exact_domain=exact_domain,
            countries=countries,
            tiers=tiers,
            current_platforms=current_platforms,
            recent_platforms=recent_platforms,
            removed_platforms=removed_platforms,
            verticals=verticals,
            sales_buckets=sales_buckets,
            timeline_platforms=timeline_platforms,
            timeline_event_types=timeline_event_types,
            timeline_date_field=effective_timeline_date_field,
            timeline_seen_from=effective_timeline_seen_from,
            timeline_seen_to=effective_timeline_seen_to,
            cms_migration_from=cms_migration_from,
            cms_migration_to=cms_migration_to,
            domain_migration_from=domain_migration_from,
            domain_migration_to=domain_migration_to,
            migration_timing_operator=effective_migration_timing_operator,
            migration_only=migration_only,
            has_domain_migration=has_domain_migration,
            has_cms_migration=has_cms_migration,
            domain_migration_statuses=domain_migration_statuses,
            domain_confidence_bands=domain_confidence_bands,
            domain_fingerprint_strengths=domain_fingerprint_strengths,
            domain_tld_relationships=domain_tld_relationships,
            cms_migration_statuses=cms_migration_statuses,
            cms_confidence_levels=cms_confidence_levels,
            has_contact=has_contact,
            has_marketing=has_marketing,
            has_crm=has_crm,
            has_payments=has_payments,
            selected_only=selected_only,
            has_seranking_analysis=has_seranking_analysis,
            seranking_analysis_types=seranking_analysis_types,
            seranking_outcome_flags=seranking_outcome_flags,
        )
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
    timeline_platforms: list[str] = Query(default=[]),
    timeline_event_types: list[str] = Query(default=[]),
    timeline_date_field: str = "first_seen",
    timeline_seen_from: str | None = None,
    timeline_seen_to: str | None = None,
    cms_migration_from: str | None = None,
    cms_migration_to: str | None = None,
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
    selected_only: bool = False,
    has_seranking_analysis: bool = False,
    seranking_analysis_types: list[str] = Query(default=[]),
    seranking_outcome_flags: list[str] = Query(default=[]),
) -> dict[str, Any]:
    effective_timeline_seen_from = timeline_seen_from or started_from
    effective_timeline_seen_to = timeline_seen_to or started_to
    effective_timeline_date_field = normalize_timeline_date_field(timeline_date_field)
    effective_migration_timing_operator = normalize_migration_timing_operator(migration_timing_operator)
    if granularity not in {"week", "month", "quarter"}:
        raise HTTPException(status_code=400, detail="Unsupported timeline granularity")
    if not timeline_platforms:
        return build_timeline_cohort_payload([], granularity, effective_timeline_date_field)

    connection = get_connection()
    try:
        migration_join_sql, _migration_select_sql = build_migration_join_and_select_sql()
        seranking_join_sql, _seranking_select_sql = build_seranking_join_and_select_sql()
        where, params = build_lead_filters(
            search,
            exact_domain,
            countries,
            tiers,
            current_platforms,
            recent_platforms,
            removed_platforms,
            verticals,
            sales_buckets,
            timeline_platforms,
            timeline_event_types,
            effective_timeline_date_field,
            effective_timeline_seen_from,
            effective_timeline_seen_to,
            cms_migration_from,
            cms_migration_to,
            domain_migration_from,
            domain_migration_to,
            effective_migration_timing_operator,
            migration_only,
            has_domain_migration,
            has_cms_migration,
            domain_migration_statuses,
            domain_confidence_bands,
            domain_fingerprint_strengths,
            domain_tld_relationships,
            cms_migration_statuses,
            cms_confidence_levels,
            has_contact,
            has_marketing,
            has_crm,
            has_payments,
            selected_only,
            has_seranking_analysis,
            seranking_analysis_types,
            seranking_outcome_flags,
        )
        cohort_clause = build_timeline_clause(
            "tt",
            timeline_platforms,
            timeline_event_types,
            effective_timeline_date_field,
            effective_timeline_seen_from,
            effective_timeline_seen_to,
            params,
            "cohort",
        )
        timeline_value_column = "tt.last_found" if effective_timeline_date_field == "last_seen" else "tt.first_detected"
        rows = connection.execute(
            f"""
            select tt.root_domain, tt.platform, {timeline_value_column} as date_value
            from technology_timelines tt
            join leads on leads.root_domain = tt.root_domain
            {migration_join_sql}
            {seranking_join_sql}
            where {where} and {cohort_clause}
            order by date_value asc, tt.platform asc, tt.root_domain asc
            """,
            params,
        ).fetchall()
        return build_timeline_cohort_payload(rows, granularity, effective_timeline_date_field)
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
    timeline_platforms: list[str] = Query(default=[]),
    timeline_event_types: list[str] = Query(default=[]),
    timeline_date_field: str = "first_seen",
    timeline_seen_from: str | None = None,
    timeline_seen_to: str | None = None,
    cms_migration_from: str | None = None,
    cms_migration_to: str | None = None,
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
    selected_only: bool = False,
    has_seranking_analysis: bool = False,
    seranking_analysis_types: list[str] = Query(default=[]),
    seranking_outcome_flags: list[str] = Query(default=[]),
    sort_by: str = DEFAULT_SORT_BY,
    sort_direction: str = DEFAULT_SORT_DIRECTION,
    page: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
) -> dict[str, Any]:
    safe_page = max(page, 1)
    safe_page_size = min(max(page_size, 1), MAX_PAGE_SIZE)
    effective_timeline_seen_from = timeline_seen_from or started_from
    effective_timeline_seen_to = timeline_seen_to or started_to
    effective_timeline_date_field = normalize_timeline_date_field(timeline_date_field)
    effective_migration_timing_operator = normalize_migration_timing_operator(migration_timing_operator)
    migration_join_sql, migration_select_sql = build_migration_join_and_select_sql()
    seranking_join_sql, seranking_select_sql = build_seranking_join_and_select_sql()
    where, params = build_lead_filters(
        search,
        exact_domain,
        countries,
        tiers,
        current_platforms,
        recent_platforms,
        removed_platforms,
        verticals,
        sales_buckets,
        timeline_platforms,
        timeline_event_types,
        effective_timeline_date_field,
        effective_timeline_seen_from,
        effective_timeline_seen_to,
        cms_migration_from,
        cms_migration_to,
        domain_migration_from,
        domain_migration_to,
        effective_migration_timing_operator,
        migration_only,
        has_domain_migration,
        has_cms_migration,
        domain_migration_statuses,
        domain_confidence_bands,
        domain_fingerprint_strengths,
        domain_tld_relationships,
        cms_migration_statuses,
        cms_confidence_levels,
        has_contact,
        has_marketing,
        has_crm,
        has_payments,
        selected_only,
        has_seranking_analysis,
        seranking_analysis_types,
        seranking_outcome_flags,
        apply_timeline_match=not bool(timeline_platforms),
    )
    timeline_join_sql, matched_select_sql = build_timeline_join_and_select_sql(
        timeline_platforms,
        timeline_event_types,
        effective_timeline_date_field,
        effective_timeline_seen_from,
        effective_timeline_seen_to,
        params,
    )
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
            {migration_join_sql}
            {seranking_join_sql}
            {timeline_join_sql}
            where {where}
            """,
            params,
        ).fetchone()[0]
        rows = connection.execute(
            f"""
            select leads.*, {migration_select_sql}, {seranking_select_sql}, {matched_select_sql}
            from leads
            {migration_join_sql}
            {seranking_join_sql}
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
    timeline_platforms: list[str] = Query(default=[]),
    timeline_event_types: list[str] = Query(default=[]),
    timeline_date_field: str = "first_seen",
    timeline_seen_from: str | None = None,
    timeline_seen_to: str | None = None,
    cms_migration_from: str | None = None,
    cms_migration_to: str | None = None,
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
    selected_only: bool = False,
    has_seranking_analysis: bool = False,
    seranking_analysis_types: list[str] = Query(default=[]),
    seranking_outcome_flags: list[str] = Query(default=[]),
    sort_by: str = DEFAULT_SORT_BY,
    sort_direction: str = DEFAULT_SORT_DIRECTION,
) -> StreamingResponse:
    effective_timeline_seen_from = timeline_seen_from or started_from
    effective_timeline_seen_to = timeline_seen_to or started_to
    effective_timeline_date_field = normalize_timeline_date_field(timeline_date_field)
    effective_migration_timing_operator = normalize_migration_timing_operator(migration_timing_operator)
    migration_join_sql, migration_select_sql = build_migration_join_and_select_sql()
    seranking_join_sql, seranking_select_sql = build_seranking_join_and_select_sql()
    where, params = build_lead_filters(
        search,
        exact_domain,
        countries,
        tiers,
        current_platforms,
        recent_platforms,
        removed_platforms,
        verticals,
        sales_buckets,
        timeline_platforms,
        timeline_event_types,
        effective_timeline_date_field,
        effective_timeline_seen_from,
        effective_timeline_seen_to,
        cms_migration_from,
        cms_migration_to,
        domain_migration_from,
        domain_migration_to,
        effective_migration_timing_operator,
        migration_only,
        has_domain_migration,
        has_cms_migration,
        domain_migration_statuses,
        domain_confidence_bands,
        domain_fingerprint_strengths,
        domain_tld_relationships,
        cms_migration_statuses,
        cms_confidence_levels,
        has_contact,
        has_marketing,
        has_crm,
        has_payments,
        selected_only,
        has_seranking_analysis,
        seranking_analysis_types,
        seranking_outcome_flags,
        apply_timeline_match=not bool(timeline_platforms),
    )
    timeline_join_sql, matched_select_sql = build_timeline_join_and_select_sql(
        timeline_platforms,
        timeline_event_types,
        effective_timeline_date_field,
        effective_timeline_seen_from,
        effective_timeline_seen_to,
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
            select leads.*, {migration_select_sql}, {seranking_select_sql}, {matched_select_sql}
            from leads
            {migration_join_sql}
            {seranking_join_sql}
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
        lead = connection.execute(
            f"""
            select leads.*, {migration_select_sql}, {seranking_select_sql}
            from leads
            {migration_join_sql}
            {seranking_join_sql}
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
        }
    finally:
        connection.close()
