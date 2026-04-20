from __future__ import annotations

import csv
import json
import re
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from difflib import SequenceMatcher
from pathlib import Path
from typing import Iterable


ROOT = Path("/Users/laurencedeer/Desktop/BuiltWith")
PROCESSED_DIR = ROOT / "processed"
REDIRECT_DIR = ROOT / "BuiltWith Exports" / "Domain Migration"
LEADS_PATH = PROCESSED_DIR / "leads.csv"
DB_PATH = PROCESSED_DIR / "builtwith.db"
TODAY = date.today()
UPLOAD_LIST_SIZES = (5000, 10000, 20000)
SECOND_LEVEL_PREFIXES = {"ac", "co", "com", "edu", "gov", "net", "org"}
DOMAIN_NOISE_TOKENS = SECOND_LEVEL_PREFIXES | {
    "au",
    "ca",
    "cn",
    "de",
    "eu",
    "fr",
    "hk",
    "in",
    "it",
    "jp",
    "mx",
    "my",
    "net",
    "nz",
    "org",
    "ru",
    "sg",
    "th",
    "tw",
    "uk",
    "us",
    "vn",
}
GENERIC_COMPANY_WORDS = {
    "and",
    "australia",
    "co",
    "com",
    "company",
    "corp",
    "corporation",
    "group",
    "holdings",
    "inc",
    "international",
    "limited",
    "ltd",
    "online",
    "pty",
    "shop",
    "store",
    "the",
}
JUNK_PATTERNS = {
    "adult",
    "bet",
    "bingo",
    "blackfriday",
    "bonus",
    "casino",
    "christmas",
    "coupon",
    "cybermonday",
    "freegift",
    "freebies",
    "giveaway",
    "loan",
    "magic-christmas",
    "porn",
    "promo",
    "roulette",
    "sex",
    "slot",
    "sweep",
    "viagra",
    "xmas",
    "xxx",
}
PLATFORM_DOMAIN_SUFFIXES = (
    "appspot.com",
    "myshopify.com",
    "netlify.app",
    "pages.dev",
    "shopifypreview.com",
    "vercel.app",
)
STAGING_HINTS = {
    "beta",
    "demo",
    "dev",
    "local",
    "preprod",
    "preview",
    "qa",
    "sandbox",
    "staging",
    "test",
    "uat",
    "ydpp",
}
PRIORITY_UPLOAD_BONUS = {"A": 24, "B": 16, "C": 8, "D": 0, "": 0}


@dataclass(frozen=True)
class CurrentLead:
    current_domain: str
    company: str
    country: str
    state: str
    city: str
    first_detected: str
    last_found: str
    first_indexed: str
    last_indexed: str
    current_platforms: str
    ecommerce_platforms: str
    cms_platforms: str
    technology_spend: str
    sales_revenue: str
    employees: str
    sku: str
    priority_tier: str
    total_score: str
    sales_buckets: str


@dataclass(frozen=True)
class RedirectObservation:
    current_domain: str
    old_domain: str
    old_root_domain: str
    redirect_first_detected: str
    redirect_last_detected: str
    redirect_duration_days: int
    source_platforms: str
    source_reports: str
    source_count: int


def clean_text(value: str) -> str:
    return " ".join((value or "").replace("\ufeff", "").strip().split())


def normalise_domain(value: str) -> str:
    domain = clean_text(value).lower()
    if not domain:
        return ""
    domain = domain.removeprefix("http://").removeprefix("https://")
    domain = domain.split("/", 1)[0].split("?", 1)[0].split("#", 1)[0]
    domain = domain.strip(".")
    domain = domain.removeprefix("www.")
    return domain


def extract_root_domain(domain: str) -> str:
    domain = normalise_domain(domain)
    if not domain:
        return ""
    labels = [part for part in domain.split(".") if part]
    if len(labels) <= 2:
        return domain
    if len(labels[-1]) == 2 and labels[-2] in SECOND_LEVEL_PREFIXES and len(labels) >= 3:
        return ".".join(labels[-3:])
    return ".".join(labels[-2:])


def company_slug(company: str) -> str:
    return re.sub(r"[^a-z0-9]", "", clean_text(company).lower())


def domain_core_label(domain: str) -> str:
    normalized = normalise_domain(domain)
    if not normalized:
        return ""
    if any(normalized.endswith(suffix) for suffix in PLATFORM_DOMAIN_SUFFIXES):
        return re.sub(r"[^a-z0-9]", "", normalized.split(".", 1)[0].lower())
    root_domain = extract_root_domain(normalized)
    labels = [part for part in root_domain.split(".") if part]
    if len(labels) == 1:
        return re.sub(r"[^a-z0-9]", "", labels[0].lower())
    if len(labels[-1]) == 2 and labels[-2] in SECOND_LEVEL_PREFIXES and len(labels) >= 3:
        core = labels[-3]
    else:
        core = labels[-2]
    return re.sub(r"[^a-z0-9]", "", core.lower())


def domain_tokens(domain: str) -> set[str]:
    normalized = normalise_domain(domain)
    tokens = {
        token
        for token in re.split(r"[^a-z0-9]+", normalized.lower())
        if token and token not in DOMAIN_NOISE_TOKENS and len(token) > 2
    }
    return tokens


def company_tokens(company: str) -> set[str]:
    return {
        token
        for token in re.split(r"[^a-z0-9]+", clean_text(company).lower())
        if token and token not in GENERIC_COMPANY_WORDS and len(token) > 2
    }


def parse_date(value: str) -> str:
    cleaned = clean_text(value)
    if not cleaned:
        return ""
    try:
        return date.fromisoformat(cleaned).isoformat()
    except ValueError:
        return ""


def days_between(older: str, newer: str) -> int | None:
    if not older or not newer:
        return None
    try:
        older_date = date.fromisoformat(older)
        newer_date = date.fromisoformat(newer)
    except ValueError:
        return None
    return (newer_date - older_date).days


def earliest_date(values: Iterable[str]) -> str:
    parsed = [parse_date(value) for value in values if parse_date(value)]
    return min(parsed) if parsed else ""


def latest_date(values: Iterable[str]) -> str:
    parsed = [parse_date(value) for value in values if parse_date(value)]
    return max(parsed) if parsed else ""


def brand_similarity_details(old_domain: str, current_domain: str, company: str) -> tuple[int, list[str]]:
    reasons: list[str] = []
    old_core = domain_core_label(old_domain)
    current_core = domain_core_label(current_domain)
    old_root = extract_root_domain(old_domain)
    current_root = extract_root_domain(current_domain)
    score = 0

    if old_core and current_core and old_core == current_core and old_root != current_root:
        score = max(score, 9)
        reasons.append("same core brand with TLD or market change")
    if old_core and current_core and (old_core in current_core or current_core in old_core):
        score = max(score, 8)
        reasons.append("strong domain string overlap")
    if old_core and current_core:
        ratio = SequenceMatcher(None, old_core, current_core).ratio()
        if ratio >= 0.85:
            score = max(score, 7)
            reasons.append("high domain similarity")
        elif ratio >= 0.7:
            score = max(score, 5)
            reasons.append("moderate domain similarity")
        elif ratio >= 0.55:
            score = max(score, 3)
            reasons.append("weak domain similarity")

    company_value = company_slug(company)
    if company_value:
        if old_core and old_core in company_value:
            score = max(score, 6)
            reasons.append("old domain matches company string")

    overlap = (domain_tokens(old_domain) & domain_tokens(current_domain)) - GENERIC_COMPANY_WORDS
    overlap_company = company_tokens(company) & domain_tokens(old_domain)
    if overlap:
        score = max(score, 5)
        reasons.append(f"shared domain tokens: {', '.join(sorted(overlap)[:3])}")
    if overlap_company:
        score = max(score, 5)
        reasons.append(f"company tokens overlap: {', '.join(sorted(overlap_company)[:3])}")

    return min(score, 10), reasons


def looks_like_junk_domain(old_domain: str) -> bool:
    normalized = normalise_domain(old_domain)
    joined = normalized.replace(".", "-")
    return any(pattern in joined for pattern in JUNK_PATTERNS)


def looks_like_platform_domain(old_domain: str) -> bool:
    normalized = normalise_domain(old_domain)
    if any(normalized.endswith(suffix) for suffix in PLATFORM_DOMAIN_SUFFIXES):
        return True
    return any(hint in domain_tokens(normalized) for hint in STAGING_HINTS)


def looks_like_alias_cleanup(old_domain: str, current_domain: str) -> bool:
    normalized_old = normalise_domain(old_domain)
    normalized_current = normalise_domain(current_domain)
    if normalized_old == normalized_current:
        return True
    if extract_root_domain(normalized_old) != extract_root_domain(normalized_current):
        return False
    old_core = domain_core_label(normalized_old)
    current_core = domain_core_label(normalized_current)
    return old_core == current_core


def parse_intish(value: str) -> int:
    cleaned = clean_text(value).replace("$", "").replace(",", "")
    if not cleaned:
        return 0
    try:
        return int(float(cleaned))
    except ValueError:
        return 0


def redirect_platform_from_name(file_name: str) -> str:
    match = re.match(r"report_redirect_(.+?)_au_nz_sg\.csv$", file_name)
    return match.group(1) if match else "unknown"


def load_and_clean_inputs() -> tuple[dict[str, CurrentLead], list[RedirectObservation], dict[str, object]]:
    current_leads: dict[str, CurrentLead] = {}
    with LEADS_PATH.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            current_domain = normalise_domain(row.get("root_domain", ""))
            if not current_domain:
                continue
            current_leads[current_domain] = CurrentLead(
                current_domain=current_domain,
                company=clean_text(row.get("company", "")),
                country=clean_text(row.get("country", "")),
                state=clean_text(row.get("state", "")),
                city=clean_text(row.get("city", "")),
                first_detected=parse_date(row.get("first_detected_any", "")),
                last_found=parse_date(row.get("last_found_any", "")),
                first_indexed=parse_date(row.get("first_indexed_any", "")),
                last_indexed=parse_date(row.get("last_indexed_any", "")),
                current_platforms=clean_text(row.get("likely_current_platforms", "")),
                ecommerce_platforms=clean_text(row.get("ecommerce_platforms", "")),
                cms_platforms=clean_text(row.get("cms_platforms", "")),
                technology_spend=clean_text(row.get("technology_spend", "")),
                sales_revenue=clean_text(row.get("sales_revenue", "")),
                employees=clean_text(row.get("employees", "")),
                sku=clean_text(row.get("sku", "")),
                priority_tier=clean_text(row.get("priority_tier", "")),
                total_score=clean_text(row.get("total_score", "")),
                sales_buckets=clean_text(row.get("sales_buckets", "")),
            )

    redirect_groups: dict[tuple[str, str], dict[str, object]] = {}
    redirect_files: list[dict[str, object]] = []
    unmatched_rows = 0

    for path in sorted(REDIRECT_DIR.glob("report_redirect_*_au_nz_sg.csv")):
        file_rows = 0
        matched_rows = 0
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                file_rows += 1
                current_domain = normalise_domain(row.get("Domain", ""))
                old_domain = normalise_domain(row.get("Inbound Redirect", ""))
                if not current_domain or not old_domain:
                    continue

                matched_domain = current_domain
                if matched_domain not in current_leads:
                    matched_domain = extract_root_domain(current_domain)
                if matched_domain not in current_leads:
                    unmatched_rows += 1
                    continue

                matched_rows += 1
                key = (matched_domain, old_domain)
                group = redirect_groups.setdefault(
                    key,
                    {
                        "current_domain": matched_domain,
                        "old_domain": old_domain,
                        "old_root_domain": extract_root_domain(old_domain),
                        "first_dates": [],
                        "last_dates": [],
                        "source_platforms": set(),
                        "source_reports": set(),
                        "source_count": 0,
                    },
                )
                group["first_dates"].append(parse_date(row.get("First Detected", "")))
                group["last_dates"].append(parse_date(row.get("Last Detected", "")))
                group["source_platforms"].add(redirect_platform_from_name(path.name))
                group["source_reports"].add(path.name)
                group["source_count"] = int(group["source_count"]) + 1

        redirect_files.append(
            {
                "file_name": path.name,
                "total_rows": file_rows,
                "matched_rows": matched_rows,
                "platform": redirect_platform_from_name(path.name),
            }
        )

    redirect_observations: list[RedirectObservation] = []
    for group in redirect_groups.values():
        redirect_first = earliest_date(group["first_dates"])
        redirect_last = latest_date(group["last_dates"])
        duration = days_between(redirect_first, redirect_last) or 0
        redirect_observations.append(
            RedirectObservation(
                current_domain=group["current_domain"],
                old_domain=group["old_domain"],
                old_root_domain=group["old_root_domain"],
                redirect_first_detected=redirect_first,
                redirect_last_detected=redirect_last,
                redirect_duration_days=duration,
                source_platforms=" | ".join(sorted(group["source_platforms"])),
                source_reports=" | ".join(sorted(group["source_reports"])),
                source_count=int(group["source_count"]),
            )
        )

    metadata = {
        "processed_at": datetime.now().isoformat(),
        "current_lead_count": len(current_leads),
        "redirect_pair_count": len(redirect_observations),
        "unmatched_redirect_rows": unmatched_rows,
        "redirect_files": redirect_files,
    }
    return current_leads, sorted(redirect_observations, key=lambda row: (row.current_domain, row.old_domain)), metadata


def score_migration_candidate(lead: CurrentLead, redirect: RedirectObservation, old_domains_for_current: int) -> dict[str, object]:
    base_score = 70
    timing_score = 0
    brand_similarity_score, brand_reasons = brand_similarity_details(redirect.old_domain, lead.current_domain, lead.company)
    brand_bonus = 5 if brand_similarity_score >= 6 else 2 if brand_similarity_score >= 3 else 0

    if redirect.redirect_duration_days >= 30:
        timing_score += 10
    else:
        days_since_last = days_between(redirect.redirect_last_detected, TODAY.isoformat())
        if days_since_last is not None and days_since_last <= 180:
            timing_score += 10

    current_start_dates = [lead.first_detected, lead.first_indexed]
    if any(
        gap is not None and 0 <= gap <= 365
        for gap in (days_between(redirect.redirect_first_detected, start_date) for start_date in current_start_dates if start_date)
    ):
        timing_score += 10

    junk_penalty = 25 if looks_like_junk_domain(redirect.old_domain) else 0
    platform_penalty = 15 if looks_like_platform_domain(redirect.old_domain) else 0
    alias_penalty = 10 if looks_like_alias_cleanup(redirect.old_domain, lead.current_domain) else 0

    unrelated_penalty = 0
    if brand_similarity_score == 0 and not junk_penalty and not platform_penalty:
        unrelated_penalty = 15

    confidence_score = base_score + timing_score + brand_bonus - junk_penalty - platform_penalty - alias_penalty - unrelated_penalty
    confidence_score = max(0, min(confidence_score, 100))

    if confidence_score >= 80:
        confidence_band = "High"
    elif confidence_score >= 60:
        confidence_band = "Medium"
    else:
        confidence_band = "Low"

    migration_flag = confidence_band in {"High", "Medium"}

    notes: list[str] = []
    if junk_penalty:
        notes.append("Likely junk or unrelated redirect")
    elif platform_penalty:
        notes.append("Redirect exists but looks platform-generated")
    elif alias_penalty:
        notes.append("Likely alias or canonical cleanup")
    elif brand_similarity_score >= 7 and timing_score >= 10:
        notes.append("Strong redirect evidence")
    elif brand_similarity_score >= 5:
        if extract_root_domain(redirect.old_domain) != extract_root_domain(lead.current_domain):
            notes.append("Possible acquisition / rebrand")
        else:
            notes.append("Possible ccTLD or market migration")
    else:
        notes.append("Redirect exists but domain looks unrelated")

    if old_domains_for_current > 1:
        notes.append(f"{old_domains_for_current} old domains point to this current domain")
    if brand_reasons:
        notes.append("; ".join(brand_reasons[:2]))

    return {
        "brand_similarity_score": brand_similarity_score,
        "timing_score": timing_score,
        "brand_bonus": brand_bonus,
        "junk_penalty": junk_penalty,
        "platform_penalty": platform_penalty,
        "alias_penalty": alias_penalty,
        "unrelated_penalty": unrelated_penalty,
        "confidence_score": confidence_score,
        "confidence_band": confidence_band,
        "migration_flag": "True" if migration_flag else "False",
        "notes": " | ".join(notes),
    }


def build_candidate_table(current_leads: dict[str, CurrentLead], redirects: list[RedirectObservation]) -> list[dict[str, str]]:
    old_domains_per_current = Counter(redirect.current_domain for redirect in redirects)
    candidates: list[dict[str, str]] = []

    for redirect in redirects:
        lead = current_leads[redirect.current_domain]
        scored = score_migration_candidate(lead, redirect, old_domains_per_current[redirect.current_domain])
        row = {
            "current_domain": lead.current_domain,
            "old_domain": redirect.old_domain,
            "old_root_domain": redirect.old_root_domain,
            "current_company": lead.company,
            "country": lead.country,
            "state": lead.state,
            "city": lead.city,
            "redirect_first_detected": redirect.redirect_first_detected,
            "redirect_last_detected": redirect.redirect_last_detected,
            "redirect_duration_days": str(redirect.redirect_duration_days),
            "current_first_detected": lead.first_detected,
            "current_last_found": lead.last_found,
            "current_first_indexed": lead.first_indexed,
            "current_last_indexed": lead.last_indexed,
            "current_platforms": lead.current_platforms,
            "current_priority_tier": lead.priority_tier,
            "current_total_score": lead.total_score,
            "number_of_old_domains_for_current": str(old_domains_per_current[redirect.current_domain]),
            "source_redirect_platforms": redirect.source_platforms,
            "source_redirect_reports": redirect.source_reports,
            "redirect_observation_count": str(redirect.source_count),
            "confidence_score": str(scored["confidence_score"]),
            "confidence_band": str(scored["confidence_band"]),
            "migration_flag": str(scored["migration_flag"]),
            "brand_similarity_score": str(scored["brand_similarity_score"]),
            "timing_score": str(scored["timing_score"]),
            "junk_penalty": str(scored["junk_penalty"]),
            "platform_penalty": str(scored["platform_penalty"]),
            "alias_penalty": str(scored["alias_penalty"]),
            "unrelated_penalty": str(scored["unrelated_penalty"]),
            "notes": str(scored["notes"]),
        }
        candidates.append(row)

    candidates.sort(
        key=lambda row: (
            -int(row["confidence_score"] or 0),
            -int(row["redirect_duration_days"] or 0),
            -int(row["brand_similarity_score"] or 0),
            row["old_domain"],
        )
    )
    return candidates


def candidate_sort_key(row: dict[str, str]) -> tuple[int, int, int, int]:
    penalty_total = sum(int(row[column] or 0) for column in ("junk_penalty", "platform_penalty", "alias_penalty", "unrelated_penalty"))
    return (
        int(row["confidence_score"] or 0),
        int(row["redirect_duration_days"] or 0),
        int(row["brand_similarity_score"] or 0),
        -penalty_total,
    )


def build_best_match_table(current_leads: dict[str, CurrentLead], candidates: list[dict[str, str]]) -> list[dict[str, str]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in candidates:
        grouped[row["current_domain"]].append(row)

    best_match_rows: list[dict[str, str]] = []
    for current_domain, lead in current_leads.items():
        rows = grouped.get(current_domain, [])
        if rows:
            best = max(rows, key=candidate_sort_key)
            best_match_rows.append(
                {
                    "current_domain": current_domain,
                    "best_old_domain": best["old_domain"],
                    "confidence_score": best["confidence_score"],
                    "confidence_band": best["confidence_band"],
                    "migration_flag": best["migration_flag"],
                    "number_of_old_domains_found": str(len(rows)),
                    "current_company": lead.company,
                    "country": lead.country,
                    "current_priority_tier": lead.priority_tier,
                    "current_total_score": lead.total_score,
                    "notes": best["notes"],
                }
            )
        else:
            best_match_rows.append(
                {
                    "current_domain": current_domain,
                    "best_old_domain": "",
                    "confidence_score": "0",
                    "confidence_band": "Low",
                    "migration_flag": "False",
                    "number_of_old_domains_found": "0",
                    "current_company": lead.company,
                    "country": lead.country,
                    "current_priority_tier": lead.priority_tier,
                    "current_total_score": lead.total_score,
                    "notes": "No inbound redirect found",
                }
            )

    best_match_rows.sort(
        key=lambda row: (
            -int(row["confidence_score"] or 0),
            -int(row["number_of_old_domains_found"] or 0),
            row["current_domain"],
        )
    )
    return best_match_rows


def build_old_domain_upload_ranking(candidates: list[dict[str, str]]) -> list[dict[str, str]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in candidates:
        grouped[row["old_domain"]].append(row)

    ranking_rows: list[dict[str, str]] = []
    for old_domain, rows in grouped.items():
        best = max(rows, key=candidate_sort_key)
        upload_priority_score = (
            int(best["confidence_score"] or 0)
            + PRIORITY_UPLOAD_BONUS.get(best["current_priority_tier"], 0)
            + min(int(best["redirect_duration_days"] or 0) // 30, 10)
            + (5 if int(best["brand_similarity_score"] or 0) >= 5 else 0)
            + (5 if int(best["number_of_old_domains_for_current"] or 0) > 1 else 0)
        )
        ranking_rows.append(
            {
                "old_domain": old_domain,
                "best_current_domain": best["current_domain"],
                "confidence_score": best["confidence_score"],
                "confidence_band": best["confidence_band"],
                "upload_priority_score": str(upload_priority_score),
                "current_priority_tier": best["current_priority_tier"],
                "current_total_score": best["current_total_score"],
                "brand_similarity_score": best["brand_similarity_score"],
                "redirect_duration_days": best["redirect_duration_days"],
                "notes": best["notes"],
            }
        )

    ranking_rows.sort(
        key=lambda row: (
            -int(row["upload_priority_score"] or 0),
            -int(row["confidence_score"] or 0),
            -int(row["brand_similarity_score"] or 0),
            row["old_domain"],
        )
    )
    return ranking_rows


def build_best_match_upload_ranking(best_match_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for row in best_match_rows:
        if not row["best_old_domain"]:
            continue
        upload_priority_score = (
            int(row["confidence_score"] or 0)
            + PRIORITY_UPLOAD_BONUS.get(row["current_priority_tier"], 0)
            + (5 if row["confidence_band"] == "High" else 0)
        )
        rows.append(
            {
                "old_domain": row["best_old_domain"],
                "best_current_domain": row["current_domain"],
                "confidence_score": row["confidence_score"],
                "confidence_band": row["confidence_band"],
                "upload_priority_score": str(upload_priority_score),
                "current_priority_tier": row["current_priority_tier"],
                "current_total_score": row["current_total_score"],
                "notes": row["notes"],
            }
        )
    rows.sort(
        key=lambda row: (
            -int(row["upload_priority_score"] or 0),
            -int(row["confidence_score"] or 0),
            row["best_current_domain"],
            row["old_domain"],
        )
    )
    return rows


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_txt(path: Path, values: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(values) + ("\n" if values else ""), encoding="utf-8")


def replace_sqlite_table(connection: sqlite3.Connection, table_name: str, rows: list[dict[str, str]]) -> None:
    connection.execute(f"drop table if exists {table_name}")
    if not rows:
        return
    columns = list(rows[0].keys())
    column_defs = ", ".join(f"{column} text" for column in columns)
    connection.execute(f"create table {table_name} ({column_defs})")
    placeholders = ", ".join("?" for _ in columns)
    connection.executemany(
        f"insert into {table_name} ({', '.join(columns)}) values ({placeholders})",
        ([row[column] for column in columns] for row in rows),
    )


def export_outputs(
    candidates: list[dict[str, str]],
    best_match_rows: list[dict[str, str]],
    upload_ranking_rows: list[dict[str, str]],
    best_match_upload_rows: list[dict[str, str]],
    metadata: dict[str, object],
) -> None:
    write_csv(PROCESSED_DIR / "domain_migration_candidates.csv", candidates)
    write_csv(PROCESSED_DIR / "domain_migration_best_match.csv", best_match_rows)
    write_csv(PROCESSED_DIR / "old_redirect_domains_upload_ranked.csv", upload_ranking_rows)
    write_csv(PROCESSED_DIR / "old_redirect_domains_best_match_upload_ranked.csv", best_match_upload_rows)

    for size in UPLOAD_LIST_SIZES:
        write_txt(
            REDIRECT_DIR / f"old_redirect_domains_upload_top_{size}.txt",
            [row["old_domain"] for row in best_match_upload_rows[:size]],
        )

    summary = {
        **metadata,
        "candidate_pair_count": len(candidates),
        "best_match_count": len(best_match_rows),
        "old_domain_upload_ranking_count": len(upload_ranking_rows),
        "best_match_upload_ranking_count": len(best_match_upload_rows),
        "candidate_confidence_counts": dict(Counter(row["confidence_band"] for row in candidates)),
        "best_match_confidence_counts": dict(Counter(row["confidence_band"] for row in best_match_rows if row["best_old_domain"])),
        "current_domains_with_redirects": sum(1 for row in best_match_rows if row["best_old_domain"]),
    }
    (PROCESSED_DIR / "domain_migration_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    connection = sqlite3.connect(DB_PATH)
    try:
        replace_sqlite_table(connection, "domain_migration_candidates", candidates)
        replace_sqlite_table(connection, "domain_migration_best_match", best_match_rows)
        replace_sqlite_table(connection, "old_redirect_domain_upload_ranked", upload_ranking_rows)
        replace_sqlite_table(connection, "old_redirect_domain_best_match_upload_ranked", best_match_upload_rows)
        connection.execute("create index if not exists idx_domain_migration_current on domain_migration_candidates(current_domain)")
        connection.execute("create index if not exists idx_domain_migration_old on domain_migration_candidates(old_domain)")
        connection.execute("create index if not exists idx_domain_migration_best_current on domain_migration_best_match(current_domain)")
        connection.execute("create index if not exists idx_old_redirect_upload_old on old_redirect_domain_upload_ranked(old_domain)")
        connection.execute("create index if not exists idx_old_redirect_best_match_upload_old on old_redirect_domain_best_match_upload_ranked(old_domain)")
        connection.commit()
    finally:
        connection.close()


def print_summary(current_leads: dict[str, CurrentLead], best_match_rows: list[dict[str, str]], candidates: list[dict[str, str]]) -> None:
    high = sum(1 for row in candidates if row["confidence_band"] == "High")
    medium = sum(1 for row in candidates if row["confidence_band"] == "Medium")
    low = sum(1 for row in candidates if row["confidence_band"] == "Low")
    domains_with_redirects = sum(1 for row in best_match_rows if row["best_old_domain"])
    print(f"Total current domains processed: {len(current_leads)}")
    print(f"Total domains with at least one inbound redirect: {domains_with_redirects}")
    print(f"Total high-confidence migrations: {high}")
    print(f"Total medium-confidence migrations: {medium}")
    print(f"Total low-confidence pairs: {low}")


def main() -> None:
    current_leads, redirects, metadata = load_and_clean_inputs()
    candidates = build_candidate_table(current_leads, redirects)
    best_match_rows = build_best_match_table(current_leads, candidates)
    upload_ranking_rows = build_old_domain_upload_ranking(candidates)
    best_match_upload_rows = build_best_match_upload_ranking(best_match_rows)
    export_outputs(candidates, best_match_rows, upload_ranking_rows, best_match_upload_rows, metadata)
    print_summary(current_leads, best_match_rows, candidates)


if __name__ == "__main__":
    main()
