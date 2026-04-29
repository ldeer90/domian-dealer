from __future__ import annotations

import csv
import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path
from typing import Iterable


ROOT = Path("/Users/laurencedeer/Desktop/BuiltWith")
RAW_DIR = ROOT / "BuiltWith Exports"
PROCESSED_DIR = ROOT / "processed"
SOURCE_MANIFEST_PATH = ROOT / "config" / "builtwith_source_manifest.csv"
TARGET_COUNTRIES = {"AU", "NZ", "SG"}
BAD_FILE_NAMES = {"Woocommrce Cehcout sited no longer detected APEC.csv"}
PREMIUM_HOSTS = {
    "Shopify Hosted",
    "Cloudflare Hosting",
    "Amazon",
    "AWS Global Accelerator",
    "Google Cloud",
    "WP Engine",
    "Fastly Hosted",
    "Fastly Load Balancer",
    "Pantheon",
    "Kinsta",
    "Servers Australia",
    "Vodien",
}
PLATFORM_SNAPSHOT_HINTS = {
    "shopify": {"shopify"},
    "shopify_plus": {"shopify plus", "shopify"},
    "woocommerce_checkout": {"woocommerce"},
    "bigcommerce": {"bigcommerce"},
    "magento": {"magento", "adobe commerce"},
    "magento_enterprise": {"magento enterprise", "adobe commerce", "magento"},
    "prestashop": {"prestashop"},
    "opencart": {"opencart", "ocstore"},
    "neto": {"neto", "maropost commerce cloud", "maropost commerce"},
    "wordpress": {"wordpress"},
    "wix": {"wix", "wix hosted", "wix peppyaka", "wix pepyaka"},
    "squarespace": {"squarespace"},
    "webflow": {"webflow"},
    "drupal": {"drupal", "govcms"},
    "joomla": {"joomla"},
    "duda": {"duda", "dudamobile"},
    "craft": {"craft"},
    "umbraco": {"umbraco"},
    "framer": {"framer"},
}

CMS_STATUS_PRIORITY = {
    "confirmed": 5,
    "possible": 4,
    "overlap": 3,
    "historic": 2,
    "removed_only": 1,
    "none": 0,
}
CMS_CONFIDENCE_PRIORITY = {"high": 3, "medium": 2, "low": 1, "none": 0}
FILTER_OPTIONS_STATUSES = {
    "domainMigrationStatuses": ["confirmed", "probable", "network", "weak", "none"],
    "cmsMigrationStatuses": ["confirmed", "possible", "historic", "overlap", "removed_only", "none"],
}


@dataclass(frozen=True)
class SourceMeta:
    path: Path
    file_name: str
    folder_name: str
    event_type: str
    platform: str
    include: bool = True
    confidence: str = ""
    manifest_report_type: str = ""
    manifest_notes: str = ""


@lru_cache(maxsize=1)
def load_source_manifest() -> dict[str, dict[str, str]]:
    if not SOURCE_MANIFEST_PATH.exists():
        return {}
    with SOURCE_MANIFEST_PATH.open("r", encoding="utf-8-sig", newline="") as handle:
        return {clean_text(row.get("relative_path", "")): row for row in csv.DictReader(handle) if clean_text(row.get("relative_path", ""))}


def classify_source(path: Path) -> SourceMeta:
    file_name = path.name
    name = file_name.lower()
    folder = path.parent.name.lower()
    relative_name = path.relative_to(RAW_DIR).as_posix()

    manifest_row = load_source_manifest().get(relative_name)
    if manifest_row:
        include = clean_text(manifest_row.get("include", "")).lower() == "true"
        event_type = clean_text(manifest_row.get("report_type", "")) if include else "excluded"
        platform = clean_text(manifest_row.get("primary_cms", "")) or "unknown"
        return SourceMeta(
            path=path,
            file_name=file_name,
            folder_name=path.parent.name,
            event_type=event_type,
            platform=platform,
            include=include,
            confidence=clean_text(manifest_row.get("confidence", "")),
            manifest_report_type=clean_text(manifest_row.get("report_type", "")),
            manifest_notes=clean_text(manifest_row.get("notes", "")),
        )

    if "recently" in folder or "recently" in name:
        event_type = "recently_added"
    elif "no longer" in folder or "no longer" in name:
        event_type = "no_longer_detected"
    elif "current" in folder or "current" in name:
        event_type = "current_detected"
    elif path.as_posix().lower().find("/new cms exports/") != -1 and ("websites in " in name or name.startswith("au_")):
        event_type = "current_detected"
    else:
        event_type = "unknown"

    platform = "unknown"
    labels = [
        ("wordpress", "wordpress"),
        ("squarespace", "squarespace"),
        ("webflow", "webflow"),
        ("drupal", "drupal"),
        ("joomla", "joomla"),
        ("duda", "duda"),
        ("craft", "craft"),
        ("umbraco", "umbraco"),
        ("framer", "framer"),
        ("wix", "wix"),
        ("shopify plus", "shopify_plus"),
        ("woocommerce checkout", "woocommerce_checkout"),
        ("woocommrce cehcout", "woocommerce_checkout"),
        ("bigcommerce", "bigcommerce"),
        ("magento enterprise", "magento_enterprise"),
        ("magento", "magento"),
        ("prestashop", "prestashop"),
        ("opencart", "opencart"),
        ("neto", "neto"),
        ("shopify", "shopify"),
    ]
    if platform == "unknown":
        for needle, label in labels:
            if needle in name:
                platform = label
                break

    return SourceMeta(
        path=path,
        file_name=file_name,
        folder_name=path.parent.name,
        event_type=event_type,
        platform=platform,
    )


def clean_text(value: str) -> str:
    return " ".join((value or "").replace("\ufeff", "").strip().split())


def split_multi(value: str) -> list[str]:
    cleaned = clean_text(value)
    if not cleaned:
        return []
    return [part.strip() for part in cleaned.split(";") if part.strip()]


def normalize_domain(value: str) -> str:
    domain = clean_text(value).lower()
    domain = domain.removeprefix("http://").removeprefix("https://")
    return domain.strip("/")


def parse_intish(value: str) -> int | None:
    cleaned = clean_text(value).replace("$", "").replace(",", "")
    if not cleaned:
        return None
    try:
        return int(float(cleaned))
    except ValueError:
        return None


def parse_date(value: str) -> str:
    cleaned = clean_text(value)
    if not cleaned:
        return ""
    return cleaned


def date_gap_days(older: str, newer: str) -> int | None:
    if not older or not newer:
        return None
    try:
        old_date = datetime.fromisoformat(older).date()
        new_date = datetime.fromisoformat(newer).date()
    except ValueError:
        return None
    return (new_date - old_date).days


def midpoint_iso_date(first_date: str, second_date: str) -> str:
    if not first_date and not second_date:
        return ""
    if not first_date:
        return second_date
    if not second_date:
        return first_date
    try:
        first_ts = datetime.fromisoformat(first_date).timestamp()
        second_ts = datetime.fromisoformat(second_date).timestamp()
    except ValueError:
        return second_date or first_date
    return datetime.fromtimestamp((first_ts + second_ts) / 2, tz=UTC).date().isoformat()


def join_pipe(values: Iterable[str]) -> str:
    return " | ".join(sorted({clean_text(value) for value in values if clean_text(value)}))


def timeline_window_for_gap(gap_days: int | None) -> str:
    if gap_days is None:
        return "unknown"
    if gap_days < 0:
        return "overlap"
    if gap_days <= 120:
        return "recent"
    if gap_days <= 365:
        return "warm"
    return "historic"


def status_for_gap(gap_days: int | None, new_has_current: bool, new_has_recent: bool) -> tuple[str, str, list[str], list[str], str]:
    warnings: list[str] = []
    evidence: list[str] = ["removed_platform_seen"]
    if new_has_current:
        evidence.append("current_platform_seen")
    if new_has_recent:
        evidence.append("recent_addition_seen")

    if gap_days is None:
        if new_has_current or new_has_recent:
            if new_has_current and not new_has_recent:
                warnings.append("missing_recent_addition")
            return (
                "possible",
                "medium",
                warnings,
                evidence,
                "Old platform disappeared and a replacement platform is now visible, but the timing window is incomplete.",
            )
        return (
            "removed_only",
            "low",
            warnings,
            evidence,
            "Old platform was removed, but no credible replacement platform is visible yet.",
        )

    if gap_days < 0:
        warnings.extend(["negative_gap", "current_removed_overlap_only"])
        return (
            "overlap",
            "low",
            warnings,
            evidence,
            "Old and new platforms overlap in BuiltWith chronology, so this looks ambiguous rather than a clean switch.",
        )

    if gap_days <= 90 and new_has_recent:
        evidence.append("tight_timing")
        return (
            "confirmed",
            "high",
            warnings,
            evidence,
            "Old and new platform timing forms a tight handover, with a recent-addition signal on the new platform.",
        )

    if gap_days <= 90 and new_has_current:
        warnings.append("missing_recent_addition")
        evidence.append("tight_timing")
        return (
            "possible",
            "medium",
            warnings,
            evidence,
            "Timing strongly suggests a switch, but the recent-addition signal is missing on the new platform.",
        )

    if gap_days <= 365:
        if new_has_current and not new_has_recent:
            warnings.append("missing_recent_addition")
        return (
            "possible",
            "medium",
            warnings,
            evidence,
            "Old and new platforms line up in a plausible migration window, but the handover is not tight enough to treat as confirmed.",
        )

    if new_has_current and not new_has_recent:
        warnings.append("missing_recent_addition")
    return (
        "historic",
        "low" if not new_has_recent else "medium",
        warnings,
        evidence,
        "Old and new platforms appear related, but the likely switch happened too far back to present as a fresh migration.",
    )


def score_contact(row: dict[str, str]) -> int:
    score = 0
    if clean_text(row["emails"]):
        score += 3
    if clean_text(row["telephones"]):
        score += 2
    if clean_text(row["people"]):
        score += 2
    if clean_text(row["linkedin"]):
        score += 1
    if clean_text(row["verified_profiles"]):
        score += 1
    return min(score, 10)


def score_stack(row: dict[str, str]) -> int:
    score = 0
    if clean_text(row["ecommerce_platform"]):
        score += 3
    if clean_text(row["payment_platforms"]):
        score += 2
    if clean_text(row["crm_platform"]):
        score += 2
    if clean_text(row["marketing_automation_platform"]):
        score += 2
    if clean_text(row["hosting_provider"]):
        score += 1
    return min(score, 10)


def tier_for(total_score: int) -> str:
    if total_score >= 17:
        return "A"
    if total_score >= 12:
        return "B"
    if total_score >= 7:
        return "C"
    return "D"


def geo_confidence(root_domain: str, country: str) -> str:
    domain = root_domain.lower()
    target_tld = ""
    if domain.endswith(".com.au") or domain.endswith(".au"):
        target_tld = "AU"
    elif domain.endswith(".co.nz") or domain.endswith(".nz"):
        target_tld = "NZ"
    elif domain.endswith(".com.sg") or domain.endswith(".sg"):
        target_tld = "SG"

    if target_tld and target_tld == country:
        return "tld_match"
    if target_tld and target_tld != country:
        return "tld_mismatch"
    return "country_only"


def preferred_value(values: Iterable[str]) -> str:
    ranked = [clean_text(value) for value in values if clean_text(value)]
    if not ranked:
        return ""
    counts = Counter(ranked)
    return max(counts, key=lambda item: (counts[item], len(item)))


def latest_date(values: Iterable[str]) -> str:
    parsed = [parse_date(value) for value in values if parse_date(value)]
    return max(parsed) if parsed else ""


def earliest_date(values: Iterable[str]) -> str:
    parsed = [parse_date(value) for value in values if parse_date(value)]
    return min(parsed) if parsed else ""


def snapshot_conflicts_with_platform(platform: str, snapshot_values: Iterable[str]) -> bool:
    if not platform or platform == "unknown":
        return False
    snapshot_text = " ".join(clean_text(value).lower() for value in snapshot_values if clean_text(value))
    if not snapshot_text:
        return False
    own_hints = PLATFORM_SNAPSHOT_HINTS.get(platform, {platform.replace("_", " ")})
    if any(hint in snapshot_text for hint in own_hints):
        return False
    return any(
        any(hint in snapshot_text for hint in hints)
        for candidate_platform, hints in PLATFORM_SNAPSHOT_HINTS.items()
        if candidate_platform != platform
    )


def cms_pair_sort_key(row: dict[str, str]) -> tuple[int, int, int, int, str]:
    status = row.get("migration_status", "none")
    confidence = row.get("confidence_level", "none")
    gap_text = clean_text(row.get("gap_days", ""))
    try:
        gap_days = int(gap_text)
    except ValueError:
        gap_days = 999999
    warning_count = len(split_multi(row.get("warning_flags", "")))
    return (
        CMS_STATUS_PRIORITY.get(status, 0),
        CMS_CONFIDENCE_PRIORITY.get(confidence, 0),
        -warning_count,
        -abs(gap_days),
        row.get("new_platform", ""),
    )


def load_rows() -> tuple[list[dict[str, str]], list[dict[str, str]], dict[str, object]]:
    source_files = sorted(path for path in RAW_DIR.rglob("*.csv") if path.name not in BAD_FILE_NAMES)
    platform_events: list[dict[str, str]] = []
    summary_files: list[dict[str, object]] = []
    invalid_rows = 0

    for path in source_files:
        meta = classify_source(path)
        file_rows = 0
        target_rows = 0
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for raw in reader:
                file_rows += 1
                if not meta.include:
                    continue
                root_domain = normalize_domain(raw.get("Root Domain", ""))
                country = clean_text(raw.get("Country", "")).upper()
                if not root_domain:
                    invalid_rows += 1
                    continue
                if country not in TARGET_COUNTRIES:
                    continue

                target_rows += 1
                platform_events.append(
                    {
                        "root_domain": root_domain,
                        "location_on_site": clean_text(raw.get("Location on Site", "")),
                        "primary_domain": normalize_domain(raw.get("Primary Domain", "")),
                        "country": country,
                        "company": clean_text(raw.get("Company", "")),
                        "vertical": clean_text(raw.get("Vertical", "")),
                        "city": clean_text(raw.get("City", "")),
                        "state": clean_text(raw.get("State", "")),
                        "zip_code": clean_text(raw.get("Zip", "")),
                        "technology_spend": str(parse_intish(raw.get("Technology Spend", "")) or ""),
                        "sales_revenue": str(parse_intish(raw.get("Sales Revenue", "")) or ""),
                        "employees": str(parse_intish(raw.get("Employees", "")) or ""),
                        "social": str(parse_intish(raw.get("Social", "")) or ""),
                        "sku": str(parse_intish(raw.get("SKU", "")) or ""),
                        "telephones": clean_text(raw.get("Telephones", "")),
                        "emails": clean_text(raw.get("Emails", "")),
                        "x_url": clean_text(raw.get("X", "")),
                        "twitter": clean_text(raw.get("Twitter", "")),
                        "facebook": clean_text(raw.get("Facebook", "")),
                        "linkedin": clean_text(raw.get("LinkedIn", "")),
                        "people": clean_text(raw.get("People", "")),
                        "verified_profiles": clean_text(raw.get("Verified Profiles", "")),
                        "first_detected": parse_date(raw.get("First Detected", "")),
                        "last_found": parse_date(raw.get("Last Found", "")),
                        "first_indexed": parse_date(raw.get("First Indexed", "")),
                        "last_indexed": parse_date(raw.get("Last Indexed", "")),
                        "ecommerce_platform": clean_text(raw.get("eCommerce Platform", "")),
                        "cms_platform": clean_text(raw.get("CMS Platform", "")),
                        "snapshot_ecommerce_platform": clean_text(raw.get("eCommerce Platform", "")),
                        "snapshot_cms_platform": clean_text(raw.get("CMS Platform", "")),
                        "crm_platform": clean_text(raw.get("CRM Platform", "")),
                        "marketing_automation_platform": clean_text(raw.get("Marketing Automation Platform", "")),
                        "payment_platforms": clean_text(raw.get("Payment Platforms", "")),
                        "crux_rank": clean_text(raw.get("CRuX Rank", "")),
                        "cloudflare_rank": clean_text(raw.get("Cloudflare Rank", "")),
                        "agency": clean_text(raw.get("Agency", "")),
                        "hosting_provider": clean_text(raw.get("Hosting Provider", "")),
                        "ai": clean_text(raw.get("AI", "")),
                        "exclusion": clean_text(raw.get("Exclusion", "")),
                        "compliance": clean_text(raw.get("Compliance", "")),
                        "event_type": meta.event_type,
                        "platform": meta.platform,
                        "source_event_type": meta.event_type,
                        "source_platform": meta.platform,
                        "source_file": meta.file_name,
                        "source_folder": meta.folder_name,
                    }
                )

        summary_files.append(
            {
                "file_name": meta.file_name,
                "folder_name": meta.folder_name,
                "event_type": meta.event_type,
                "platform": meta.platform,
                "include": meta.include,
                "confidence": meta.confidence,
                "manifest_report_type": meta.manifest_report_type,
                "notes": meta.manifest_notes,
                "total_rows": file_rows,
                "target_rows": target_rows,
            }
        )

    processed_at = datetime.now(UTC).isoformat()
    metadata = {
        "processed_at": processed_at,
        "target_countries": sorted(TARGET_COUNTRIES),
        "excluded_files": sorted(BAD_FILE_NAMES),
        "valid_source_file_count": len(summary_files),
        "invalid_rows_without_domain": invalid_rows,
        "source_files": summary_files,
        "source_coverage": build_source_coverage(summary_files),
    }
    return platform_events, summary_files, metadata


def build_source_coverage(summary_files: list[dict[str, object]]) -> list[dict[str, object]]:
    service_platforms = ["wordpress", "wix", "squarespace", "webflow", "drupal", "joomla", "duda", "craft", "umbraco", "framer"]
    coverage: dict[str, dict[str, object]] = {
        platform: {
            "platform": platform,
            "hasCurrent": False,
            "hasRecent": False,
            "hasRemoved": False,
            "currentFiles": 0,
            "recentFiles": 0,
            "removedFiles": 0,
            "quarantinedFiles": 0,
            "rowCount": 0,
            "targetRows": 0,
            "confidence": "none",
            "timingQuality": "untrusted",
            "notes": [],
        }
        for platform in service_platforms
    }
    confidence_rank = {"none": 0, "low": 1, "needs_review": 2, "medium": 3, "high": 4}

    for source in summary_files:
        platform = clean_text(str(source.get("platform", "")))
        source_platform = clean_text(str(source.get("source_platform", "")))
        if not platform or platform == "unknown":
            platform = source_platform
        if platform not in coverage:
            continue

        entry = coverage[platform]
        include = bool(source.get("include", True))
        event_type = clean_text(str(source.get("event_type", "")))
        target_rows = int(source.get("target_rows", 0) or 0)
        total_rows = int(source.get("total_rows", 0) or 0)
        entry["rowCount"] = int(entry["rowCount"]) + total_rows
        entry["targetRows"] = int(entry["targetRows"]) + target_rows

        confidence = clean_text(str(source.get("confidence", ""))) or "none"
        if confidence_rank.get(confidence, 0) > confidence_rank.get(str(entry["confidence"]), 0):
            entry["confidence"] = confidence

        if not include:
            entry["quarantinedFiles"] = int(entry["quarantinedFiles"]) + 1
            continue

        if event_type == "current_detected":
            entry["hasCurrent"] = True
            entry["currentFiles"] = int(entry["currentFiles"]) + 1
        elif event_type == "recently_added":
            entry["hasRecent"] = True
            entry["recentFiles"] = int(entry["recentFiles"]) + 1
        elif event_type == "no_longer_detected":
            entry["hasRemoved"] = True
            entry["removedFiles"] = int(entry["removedFiles"]) + 1

    for entry in coverage.values():
        if entry["hasCurrent"] and entry["hasRecent"] and entry["hasRemoved"]:
            entry["timingQuality"] = "complete"
        elif entry["hasCurrent"] and entry["hasRemoved"]:
            entry["timingQuality"] = "partial"
        elif entry["hasCurrent"] and entry["hasRecent"]:
            entry["timingQuality"] = "partial_recent_only"
        elif entry["hasCurrent"]:
            entry["timingQuality"] = "current_only"
        else:
            entry["timingQuality"] = "untrusted"

        notes: list[str] = []
        if not entry["hasRemoved"]:
            notes.append("Missing no-longer-detected export")
        if not entry["hasRecent"]:
            notes.append("Missing recently-added export")
        if entry["quarantinedFiles"]:
            notes.append(f"{entry['quarantinedFiles']} quarantined file(s)")
        entry["notes"] = notes

    return list(coverage.values())


def build_technology_timelines(platform_events: list[dict[str, str]]) -> list[dict[str, str]]:
    grouped: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for row in platform_events:
        platform = row["platform"]
        if not platform or platform == "unknown":
            continue
        grouped[(row["root_domain"], platform)].append(row)

    timelines: list[dict[str, str]] = []
    for (root_domain, platform), rows in grouped.items():
        event_types = sorted({row["event_type"] for row in rows if row["event_type"] and row["event_type"] != "unknown"})
        timelines.append(
            {
                "root_domain": root_domain,
                "platform": platform,
                "first_detected": earliest_date(row["first_detected"] for row in rows),
                "last_found": latest_date(row["last_found"] for row in rows),
                "first_indexed": earliest_date(row["first_indexed"] for row in rows),
                "last_indexed": latest_date(row["last_indexed"] for row in rows),
                "has_current_detected": "1" if any(row["event_type"] == "current_detected" for row in rows) else "0",
                "has_recently_added": "1" if any(row["event_type"] == "recently_added" for row in rows) else "0",
                "has_removed": "1" if any(row["event_type"] == "no_longer_detected" for row in rows) else "0",
                "event_types": " | ".join(event_types),
            }
        )

    timelines.sort(key=lambda row: (row["root_domain"], row["platform"]))
    return timelines


def build_leads(
    platform_events: list[dict[str, str]],
) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]], dict[str, object], dict[str, object]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in platform_events:
        grouped[row["root_domain"]].append(row)

    leads: list[dict[str, str]] = []
    migration_pairs: list[dict[str, str]] = []
    cms_migration_pairs_v2: list[dict[str, str]] = []
    country_counter: Counter[str] = Counter()
    platform_counter: Counter[str] = Counter()
    corridor_counter: Counter[str] = Counter()
    geo_counter: Counter[str] = Counter()
    bucket_counter: Counter[str] = Counter()
    cms_status_counter: Counter[str] = Counter()
    lead_integrity_counter: Counter[str] = Counter()
    integrity_samples: dict[str, list[str]] = defaultdict(list)

    for root_domain, rows in grouped.items():
        countries = Counter(row["country"] for row in rows if row["country"])
        primary_country = countries.most_common(1)[0][0] if countries else ""
        country_counter[primary_country] += 1
        geo_flag = geo_confidence(root_domain, primary_country)
        geo_counter[geo_flag] += 1

        recent_platforms = sorted({row["platform"] for row in rows if row["event_type"] == "recently_added"})
        removed_platforms = sorted({row["platform"] for row in rows if row["event_type"] == "no_longer_detected"})
        current_platforms = sorted({row["platform"] for row in rows if row["event_type"] == "current_detected"})
        current_candidate_platforms = sorted(set(current_platforms) | set(recent_platforms))

        for platform in recent_platforms:
            platform_counter[f"recently_added:{platform}"] += 1
        for platform in removed_platforms:
            platform_counter[f"no_longer_detected:{platform}"] += 1
        for platform in current_platforms:
            platform_counter[f"current_detected:{platform}"] += 1

        legacy_confidence = "high" if recent_platforms and removed_platforms else "medium" if recent_platforms or removed_platforms else "low"
        legacy_gap_days: list[int] = []
        if recent_platforms and removed_platforms:
            for old_platform in removed_platforms:
                for new_platform in current_candidate_platforms:
                    if old_platform == new_platform:
                        continue
                    old_last_found = latest_date(
                        row["last_found"]
                        for row in rows
                        if row["platform"] == old_platform and row["event_type"] == "no_longer_detected"
                    )
                    new_first_detected = earliest_date(
                        row["first_detected"]
                        for row in rows
                        if row["platform"] == new_platform and row["event_type"] in {"recently_added", "current_detected"}
                    )
                    gap_days = date_gap_days(old_last_found, new_first_detected)
                    if gap_days is not None:
                        legacy_gap_days.append(gap_days)
                    migration_pairs.append(
                        {
                            "root_domain": root_domain,
                            "country": primary_country,
                            "old_platform": old_platform,
                            "new_platform": new_platform,
                            "confidence_level": legacy_confidence,
                            "first_new_detected": new_first_detected,
                            "last_old_found": old_last_found,
                            "gap_days": str(gap_days if gap_days is not None else ""),
                        }
                    )

        lead_integrity_flags: set[str] = set()
        if current_platforms and recent_platforms and set(current_platforms) != set(recent_platforms):
            lead_integrity_flags.add("current_recent_mismatch")

        conflicting_snapshot = any(
            snapshot_conflicts_with_platform(
                row["platform"],
                [row.get("snapshot_ecommerce_platform", ""), row.get("snapshot_cms_platform", "")],
            )
            for row in rows
            if row["event_type"] == "no_longer_detected"
        )
        if conflicting_snapshot:
            lead_integrity_flags.add("conflicting_platform_snapshot")

        cms_pairs_for_lead: list[dict[str, str]] = []
        for old_platform in removed_platforms:
            old_last_found = latest_date(
                row["last_found"]
                for row in rows
                if row["platform"] == old_platform and row["event_type"] == "no_longer_detected"
            )
            new_candidates = [platform for platform in current_candidate_platforms if platform != old_platform]
            if not new_candidates:
                warning_flags = []
                if conflicting_snapshot:
                    warning_flags.append("conflicting_platform_snapshot")
                pair = {
                    "root_domain": root_domain,
                    "country": primary_country,
                    "old_platform": old_platform,
                    "new_platform": "",
                    "migration_status": "removed_only",
                    "confidence_level": "low",
                    "migration_reason": "Old platform was removed, but no credible replacement platform is visible yet.",
                    "first_new_detected": "",
                    "last_old_found": old_last_found,
                    "likely_migration_date": old_last_found,
                    "gap_days": "",
                    "migration_window": "removed_only",
                    "warning_flags": join_pipe(warning_flags),
                    "evidence_flags": "removed_platform_seen",
                }
                cms_pairs_for_lead.append(pair)
                cms_status_counter["removed_only"] += 1
                continue

            for new_platform in new_candidates:
                new_first_detected = earliest_date(
                    row["first_detected"]
                    for row in rows
                    if row["platform"] == new_platform and row["event_type"] in {"recently_added", "current_detected"}
                )
                gap_days = date_gap_days(old_last_found, new_first_detected)
                status, confidence, warning_flags, evidence_flags, reason = status_for_gap(
                    gap_days,
                    new_platform in current_platforms,
                    new_platform in recent_platforms,
                )
                if conflicting_snapshot and "conflicting_platform_snapshot" not in warning_flags:
                    warning_flags.append("conflicting_platform_snapshot")
                cms_pair = {
                    "root_domain": root_domain,
                    "country": primary_country,
                    "old_platform": old_platform,
                    "new_platform": new_platform,
                    "migration_status": status,
                    "confidence_level": confidence,
                    "migration_reason": reason,
                    "first_new_detected": new_first_detected,
                    "last_old_found": old_last_found,
                    "likely_migration_date": midpoint_iso_date(old_last_found, new_first_detected),
                    "gap_days": str(gap_days if gap_days is not None else ""),
                    "migration_window": timeline_window_for_gap(gap_days),
                    "warning_flags": join_pipe(warning_flags),
                    "evidence_flags": join_pipe(evidence_flags),
                }
                cms_pairs_for_lead.append(cms_pair)
                cms_status_counter[status] += 1
                if new_platform and status in {"confirmed", "possible", "historic"}:
                    corridor_counter[f"{old_platform}->{new_platform}"] += 1
                for warning_flag in warning_flags:
                    lead_integrity_flags.add(warning_flag)

        if removed_platforms and current_candidate_platforms and not recent_platforms:
            lead_integrity_flags.add("missing_recent_addition")

        best_cms_pair = max(cms_pairs_for_lead, key=cms_pair_sort_key, default=None)
        closest_gap = None
        migration_window = "none"
        if best_cms_pair is not None:
            try:
                closest_gap = int(best_cms_pair["gap_days"])
            except ValueError:
                closest_gap = None
            migration_window = best_cms_pair.get("migration_window", "none") or "none"

        aggregate = {
            "root_domain": root_domain,
            "company": preferred_value(row["company"] for row in rows),
            "country": primary_country,
            "geo_confidence": geo_flag,
            "state": preferred_value(row["state"] for row in rows),
            "city": preferred_value(row["city"] for row in rows),
            "vertical": preferred_value(row["vertical"] for row in rows),
            "technology_spend": str(max((int(row["technology_spend"]) for row in rows if row["technology_spend"]), default=0) or ""),
            "sales_revenue": str(max((int(row["sales_revenue"]) for row in rows if row["sales_revenue"]), default=0) or ""),
            "employees": str(max((int(row["employees"]) for row in rows if row["employees"]), default=0) or ""),
            "social": str(max((int(row["social"]) for row in rows if row["social"]), default=0) or ""),
            "sku": str(max((int(row["sku"]) for row in rows if row["sku"]), default=0) or ""),
            "emails": join_pipe(item for row in rows for item in split_multi(row["emails"])),
            "telephones": join_pipe(item for row in rows for item in split_multi(row["telephones"])),
            "people": join_pipe(item for row in rows for item in split_multi(row["people"])),
            "linkedin": preferred_value(row["linkedin"] for row in rows),
            "verified_profiles": join_pipe(item for row in rows for item in split_multi(row["verified_profiles"])),
            "recently_added_platforms": join_pipe(recent_platforms),
            "removed_platforms": join_pipe(removed_platforms),
            "current_platforms": join_pipe(current_platforms),
            "current_candidate_platforms": join_pipe(current_candidate_platforms),
            "likely_current_platforms": join_pipe(current_candidate_platforms),
            "ecommerce_platforms": join_pipe(item for row in rows for item in split_multi(row["ecommerce_platform"])),
            "cms_platforms": join_pipe(item for row in rows for item in split_multi(row["cms_platform"])),
            "crm_platforms": join_pipe(item for row in rows for item in split_multi(row["crm_platform"])),
            "marketing_platforms": join_pipe(item for row in rows for item in split_multi(row["marketing_automation_platform"])),
            "payment_platforms": join_pipe(item for row in rows for item in split_multi(row["payment_platforms"])),
            "hosting_providers": join_pipe(item for row in rows for item in split_multi(row["hosting_provider"])),
            "agencies": join_pipe(item for row in rows for item in split_multi(row["agency"])),
            "ai_tools": join_pipe(item for row in rows for item in split_multi(row["ai"])),
            "compliance_flags": join_pipe(item for row in rows for item in split_multi(row["compliance"])),
            "first_detected_any": earliest_date(row["first_detected"] for row in rows),
            "last_found_any": latest_date(row["last_found"] for row in rows),
            "first_indexed_any": earliest_date(row["first_indexed"] for row in rows),
            "last_indexed_any": latest_date(row["last_indexed"] for row in rows),
            "event_count": str(len(rows)),
            "has_recent_addition": "1" if recent_platforms else "0",
            "has_removal": "1" if removed_platforms else "0",
            "migration_candidate_flag": "1" if any(pair["migration_status"] != "removed_only" for pair in cms_pairs_for_lead) else "0",
            "migration_window": migration_window,
            "closest_migration_gap_days": str(closest_gap if closest_gap is not None else ""),
            "integrity_flags": join_pipe(lead_integrity_flags),
            "contact_score": "0",
            "stack_score": "0",
            "trigger_score": "0",
            "total_score": "0",
            "priority_tier": "",
            "sales_buckets": "",
            "bucket_reasons": "",
            "cms_migration_status": best_cms_pair["migration_status"] if best_cms_pair else "none",
            "cms_migration_confidence": best_cms_pair["confidence_level"] if best_cms_pair else "none",
            "cms_migration_reason": best_cms_pair["migration_reason"] if best_cms_pair else "",
            "cms_migration_old_platform": best_cms_pair["old_platform"] if best_cms_pair else "",
            "cms_migration_new_platform": best_cms_pair["new_platform"] if best_cms_pair else "",
            "cms_migration_first_new_seen": best_cms_pair["first_new_detected"] if best_cms_pair else "",
            "cms_migration_last_old_seen": best_cms_pair["last_old_found"] if best_cms_pair else "",
            "cms_migration_gap_days": best_cms_pair["gap_days"] if best_cms_pair else "",
            "cms_migration_likely_date": best_cms_pair["likely_migration_date"] if best_cms_pair else "",
            "cms_migration_warning_flags": best_cms_pair["warning_flags"] if best_cms_pair else "",
            "cms_migration_evidence_flags": best_cms_pair["evidence_flags"] if best_cms_pair else "",
            "cms_migration_candidate_count": str(len(cms_pairs_for_lead)),
        }

        contact_score = score_contact(
            {
                "emails": aggregate["emails"],
                "telephones": aggregate["telephones"],
                "people": aggregate["people"],
                "linkedin": aggregate["linkedin"],
                "verified_profiles": aggregate["verified_profiles"],
            }
        )
        stack_score = score_stack(
            {
                "ecommerce_platform": aggregate["ecommerce_platforms"],
                "payment_platforms": aggregate["payment_platforms"],
                "crm_platform": aggregate["crm_platforms"],
                "marketing_automation_platform": aggregate["marketing_platforms"],
                "hosting_provider": aggregate["hosting_providers"],
            }
        )
        trigger_score = 0
        if recent_platforms:
            trigger_score += 4
        if removed_platforms:
            trigger_score += 1
        if best_cms_pair:
            if best_cms_pair["migration_status"] == "confirmed":
                trigger_score += 5
            elif best_cms_pair["migration_status"] == "possible":
                trigger_score += 3
            elif best_cms_pair["migration_status"] == "historic":
                trigger_score += 1
        total_score = contact_score + stack_score + trigger_score

        aggregate["contact_score"] = str(contact_score)
        aggregate["stack_score"] = str(stack_score)
        aggregate["trigger_score"] = str(trigger_score)
        aggregate["total_score"] = str(total_score)
        aggregate["priority_tier"] = tier_for(total_score)

        sales_buckets: list[str] = []
        bucket_reasons: list[str] = []

        has_marketing = bool(aggregate["marketing_platforms"])
        has_crm = bool(aggregate["crm_platforms"])
        has_payments = bool(aggregate["payment_platforms"])
        has_contact = bool(aggregate["emails"] or aggregate["telephones"] or aggregate["people"])
        has_recent = bool(recent_platforms)
        has_removed = bool(removed_platforms)
        has_shopify_current = any(platform in {"shopify", "shopify_plus"} for platform in current_candidate_platforms)
        has_shopify_detected_current = "shopify" in current_platforms
        has_shopify_plus_detected_current = "shopify_plus" in current_platforms
        has_woocommerce_current = "woocommerce_checkout" in current_candidate_platforms
        has_woocommerce_removed = "woocommerce_checkout" in removed_platforms
        has_premium_hosting = any(item in PREMIUM_HOSTS for item in split_multi(aggregate["hosting_providers"]))
        tech_spend = int(aggregate["technology_spend"]) if aggregate["technology_spend"] else 0

        def add_bucket(name: str, reason: str) -> None:
            sales_buckets.append(name)
            bucket_reasons.append(f"{name}: {reason}")
            bucket_counter[name] += 1

        if has_recent:
            add_bucket("recent_platform_adopter", f"new platform {', '.join(recent_platforms)}")
        if has_removed:
            add_bucket("platform_removed_signal", f"removed {', '.join(removed_platforms)}")
        if has_shopify_detected_current:
            add_bucket("current_shopify", "currently detected Shopify")
        if has_shopify_plus_detected_current:
            add_bucket("current_shopify_plus", "currently detected Shopify Plus")
        if best_cms_pair and best_cms_pair["new_platform"]:
            if best_cms_pair["migration_status"] == "confirmed" and best_cms_pair["migration_window"] == "recent":
                add_bucket(
                    "recent_migration_signal",
                    f"{best_cms_pair['old_platform']} -> {best_cms_pair['new_platform']} within {best_cms_pair['gap_days'] or 'unknown'} days",
                )
            elif best_cms_pair["migration_status"] in {"possible", "historic"}:
                add_bucket(
                    "historic_replatform_signal",
                    f"{best_cms_pair['old_platform']} -> {best_cms_pair['new_platform']} marked {best_cms_pair['migration_status']}",
                )
        if has_woocommerce_removed:
            add_bucket("woocommerce_removed_signal", "removed WooCommerce Checkout")
        if has_shopify_current and has_removed:
            add_bucket("switch_to_shopify", f"current {', '.join(current_candidate_platforms)} after removing {', '.join(removed_platforms)}")
        if has_woocommerce_current and has_removed:
            add_bucket("switch_to_woocommerce", f"current WooCommerce after removing {', '.join(removed_platforms)}")
        if has_woocommerce_removed and "shopify" in current_candidate_platforms:
            if best_cms_pair and best_cms_pair["new_platform"] == "shopify" and best_cms_pair["migration_window"] == "recent":
                add_bucket("woo_to_shopify_recent", f"WooCommerce -> Shopify within {best_cms_pair['gap_days'] or 'unknown'} days")
            else:
                add_bucket("woo_to_shopify", "WooCommerce -> Shopify")
        if has_woocommerce_removed and "shopify_plus" in current_candidate_platforms:
            if best_cms_pair and best_cms_pair["new_platform"] == "shopify_plus" and best_cms_pair["migration_window"] == "recent":
                add_bucket("woo_to_shopify_plus_recent", f"WooCommerce -> Shopify Plus within {best_cms_pair['gap_days'] or 'unknown'} days")
            else:
                add_bucket("woo_to_shopify_plus", "WooCommerce -> Shopify Plus")
        if has_payments and (has_marketing or has_crm):
            stack_parts = []
            if has_payments:
                stack_parts.append("payments")
            if has_marketing:
                stack_parts.append("marketing")
            if has_crm:
                stack_parts.append("crm")
            add_bucket("revenue_stack", " + ".join(stack_parts))
        if has_marketing or has_crm:
            tools = split_multi(aggregate["marketing_platforms"])[:2] + split_multi(aggregate["crm_platforms"])[:2]
            add_bucket("marketing_mature", ", ".join(tools[:3]) if tools else "marketing or crm present")
        if has_contact and has_payments and (has_marketing or has_crm):
            add_bucket("contactable_revenue_stack", "direct contact plus payments plus marketing/crm")
        if has_shopify_detected_current and has_payments and (has_marketing or has_crm):
            add_bucket("current_shopify_revenue_stack", "current Shopify with payments plus marketing/crm")
        if has_shopify_detected_current and has_contact:
            add_bucket("current_shopify_contactable", "current Shopify with direct contact data")
        if has_shopify_plus_detected_current and has_contact:
            add_bucket("current_shopify_plus_contactable", "current Shopify Plus with direct contact data")
        if has_contact and aggregate["priority_tier"] in {"A", "B"}:
            add_bucket("contact_ready_ab", f"tier {aggregate['priority_tier']} with direct contact fields")
        if has_premium_hosting and aggregate["priority_tier"] in {"A", "B"}:
            add_bucket(
                "premium_hosting_ab",
                ", ".join([item for item in split_multi(aggregate["hosting_providers"]) if item in PREMIUM_HOSTS][:2]),
            )
        if has_woocommerce_removed and has_contact and has_payments and (has_marketing or has_crm):
            add_bucket("woo_removed_revenue_stack", "WooCommerce removed with contactable revenue stack")
        if has_shopify_detected_current and has_woocommerce_removed:
            add_bucket("current_shopify_removed_woocommerce", "current Shopify with WooCommerce removed signal")
        if has_shopify_plus_detected_current and has_woocommerce_removed:
            add_bucket("current_shopify_plus_removed_woocommerce", "current Shopify Plus with WooCommerce removed signal")
        if tech_spend >= 2500:
            add_bucket("high_spend", f"technology spend {tech_spend}")
        if has_shopify_detected_current and tech_spend >= 2500:
            add_bucket("current_shopify_high_spend", f"current Shopify with technology spend {tech_spend}")
        if has_shopify_plus_detected_current and tech_spend >= 2500:
            add_bucket("current_shopify_plus_high_spend", f"current Shopify Plus with technology spend {tech_spend}")
        if "shopify_plus" in current_candidate_platforms:
            add_bucket("shopify_plus_target", "Shopify Plus detected")
        if geo_flag == "tld_mismatch":
            add_bucket("geo_review_needed", f"TLD suggests another market than {primary_country}")

        aggregate["sales_buckets"] = join_pipe(sales_buckets)
        aggregate["bucket_reasons"] = " || ".join(bucket_reasons)

        leads.append(aggregate)
        cms_migration_pairs_v2.extend(cms_pairs_for_lead)

        for flag in [item.strip() for item in aggregate["integrity_flags"].split("|") if item.strip()]:
            lead_integrity_counter[flag] += 1
            if len(integrity_samples[flag]) < 10:
                integrity_samples[flag].append(root_domain)

    overview = {
        "unique_leads": len(leads),
        "country_counts": dict(country_counter),
        "geo_confidence_counts": dict(geo_counter),
        "event_platform_counts": dict(platform_counter),
        "sales_bucket_counts": dict(bucket_counter),
        "cms_migration_status_counts": dict(cms_status_counter),
        "top_corridors": [
            {"corridor": corridor, "count": count}
            for corridor, count in corridor_counter.most_common(15)
        ],
        "migration_pair_count": len(migration_pairs),
        "cms_migration_pair_v2_count": len(cms_migration_pairs_v2),
    }
    integrity_audit = {
        "generated_at": datetime.now(UTC).isoformat(),
        "lead_count": len(leads),
        "legacy_migration_pair_count": len(migration_pairs),
        "cms_migration_pair_v2_count": len(cms_migration_pairs_v2),
        "cms_migration_status_counts": dict(cms_status_counter),
        "lead_integrity_flag_counts": dict(lead_integrity_counter),
        "samples": dict(integrity_samples),
    }
    return leads, migration_pairs, cms_migration_pairs_v2, overview, integrity_audit


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def build_filter_options_payload(
    leads: list[dict[str, str]],
    platform_events: list[dict[str, str]],
    technology_timelines: list[dict[str, str]],
) -> dict[str, object]:
    bucket_counts: Counter[str] = Counter()
    countries = sorted({row["country"] for row in leads if row["country"]})
    tiers = sorted({row["priority_tier"] for row in leads if row["priority_tier"]})
    vertical_counter = Counter(row["vertical"] for row in leads if row["vertical"])

    for row in leads:
        for bucket in [item.strip() for item in row["sales_buckets"].split("|") if item.strip()]:
            bucket_counts[bucket] += 1

    def grouped_platforms(event_type: str) -> list[str]:
        counts = Counter(
            row["platform"]
            for row in platform_events
            if row["event_type"] == event_type and row["platform"] and row["platform"] != "unknown"
        )
        return [name for name, _count in counts.most_common()]

    timeline_counts = Counter(
        row["platform"]
        for row in technology_timelines
        if row["platform"] and row["platform"] != "unknown"
    )

    return {
        "countries": countries,
        "tiers": tiers,
        "verticals": [name for name, _count in vertical_counter.most_common()],
        "currentPlatforms": grouped_platforms("current_detected"),
        "recentPlatforms": grouped_platforms("recently_added"),
        "removedPlatforms": grouped_platforms("no_longer_detected"),
        "timelinePlatforms": [name for name, _count in timeline_counts.most_common()],
        "salesBuckets": [name for name, _count in sorted(bucket_counts.items(), key=lambda item: (-item[1], item[0]))],
        **FILTER_OPTIONS_STATUSES,
    }


def write_sqlite(
    path: Path,
    platform_events: list[dict[str, str]],
    technology_timelines: list[dict[str, str]],
    leads: list[dict[str, str]],
    migration_pairs: list[dict[str, str]],
    cms_migration_pairs_v2: list[dict[str, str]],
    metadata: dict[str, object],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        path.unlink()

    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row

    connection.execute(
        """
        create table leads (
            root_domain text primary key,
            company text,
            country text,
            geo_confidence text,
            state text,
            city text,
            vertical text,
            technology_spend integer,
            sales_revenue integer,
            employees integer,
            social integer,
            sku integer,
            emails text,
            telephones text,
            people text,
            linkedin text,
            verified_profiles text,
            recently_added_platforms text,
            removed_platforms text,
            current_platforms text,
            current_candidate_platforms text,
            likely_current_platforms text,
            ecommerce_platforms text,
            cms_platforms text,
            crm_platforms text,
            marketing_platforms text,
            payment_platforms text,
            hosting_providers text,
            agencies text,
            ai_tools text,
            compliance_flags text,
            first_detected_any text,
            last_found_any text,
            first_indexed_any text,
            last_indexed_any text,
            event_count integer,
            has_recent_addition integer,
            has_removal integer,
            migration_candidate_flag integer,
            migration_window text,
            closest_migration_gap_days integer,
            integrity_flags text,
            contact_score integer,
            stack_score integer,
            trigger_score integer,
            total_score integer,
            priority_tier text,
            sales_buckets text,
            bucket_reasons text,
            cms_migration_status text,
            cms_migration_confidence text,
            cms_migration_reason text,
            cms_migration_old_platform text,
            cms_migration_new_platform text,
            cms_migration_first_new_seen text,
            cms_migration_last_old_seen text,
            cms_migration_gap_days integer,
            cms_migration_likely_date text,
            cms_migration_warning_flags text,
            cms_migration_evidence_flags text,
            cms_migration_candidate_count integer
        )
        """
    )
    connection.execute(
        """
        create table platform_events (
            id integer primary key autoincrement,
            root_domain text not null,
            location_on_site text,
            primary_domain text,
            country text,
            company text,
            vertical text,
            city text,
            state text,
            zip_code text,
            technology_spend integer,
            sales_revenue integer,
            employees integer,
            social integer,
            sku integer,
            telephones text,
            emails text,
            x_url text,
            twitter text,
            facebook text,
            linkedin text,
            people text,
            verified_profiles text,
            first_detected text,
            last_found text,
            first_indexed text,
            last_indexed text,
            ecommerce_platform text,
            cms_platform text,
            snapshot_ecommerce_platform text,
            snapshot_cms_platform text,
            crm_platform text,
            marketing_automation_platform text,
            payment_platforms text,
            crux_rank text,
            cloudflare_rank text,
            agency text,
            hosting_provider text,
            ai text,
            exclusion text,
            compliance text,
            event_type text,
            platform text,
            source_event_type text,
            source_platform text,
            source_file text,
            source_folder text
        )
        """
    )
    connection.execute(
        """
        create table technology_timelines (
            id integer primary key autoincrement,
            root_domain text not null,
            platform text not null,
            first_detected text,
            last_found text,
            first_indexed text,
            last_indexed text,
            has_current_detected integer,
            has_recently_added integer,
            has_removed integer,
            event_types text
        )
        """
    )
    connection.execute(
        """
        create table cms_migration_pairs_v2 (
            id integer primary key autoincrement,
            root_domain text not null,
            country text,
            old_platform text,
            new_platform text,
            migration_status text,
            confidence_level text,
            migration_reason text,
            first_new_detected text,
            last_old_found text,
            likely_migration_date text,
            gap_days integer,
            migration_window text,
            warning_flags text,
            evidence_flags text
        )
        """
    )
    connection.execute(
        """
        create table migration_pairs (
            id integer primary key autoincrement,
            root_domain text not null,
            country text,
            old_platform text,
            new_platform text,
            confidence_level text,
            first_new_detected text,
            last_old_found text,
            gap_days integer
        )
        """
    )
    connection.execute(
        "create table metadata (key text primary key, value text not null)"
    )
    connection.execute(
        "create virtual table leads_search using fts5(root_domain, company, vertical, recently_added_platforms, removed_platforms, crm_platforms, marketing_platforms, payment_platforms, hosting_providers, content='leads', content_rowid='rowid')"
    )

    lead_columns = list(leads[0].keys()) if leads else []
    event_columns = list(platform_events[0].keys()) if platform_events else []
    timeline_columns = list(technology_timelines[0].keys()) if technology_timelines else []
    migration_columns = list(migration_pairs[0].keys()) if migration_pairs else []
    cms_v2_columns = list(cms_migration_pairs_v2[0].keys()) if cms_migration_pairs_v2 else []

    if leads:
        placeholders = ", ".join("?" for _ in lead_columns)
        connection.executemany(
            f"insert into leads ({', '.join(lead_columns)}) values ({placeholders})",
            ([row[column] for column in lead_columns] for row in leads),
        )

    if platform_events:
        placeholders = ", ".join("?" for _ in event_columns)
        connection.executemany(
            f"insert into platform_events ({', '.join(event_columns)}) values ({placeholders})",
            ([row[column] for column in event_columns] for row in platform_events),
        )

    if technology_timelines:
        placeholders = ", ".join("?" for _ in timeline_columns)
        connection.executemany(
            f"insert into technology_timelines ({', '.join(timeline_columns)}) values ({placeholders})",
            ([row[column] for column in timeline_columns] for row in technology_timelines),
        )

    if migration_pairs:
        placeholders = ", ".join("?" for _ in migration_columns)
        connection.executemany(
            f"insert into migration_pairs ({', '.join(migration_columns)}) values ({placeholders})",
            ([row[column] for column in migration_columns] for row in migration_pairs),
        )

    if cms_migration_pairs_v2:
        placeholders = ", ".join("?" for _ in cms_v2_columns)
        connection.executemany(
            f"insert into cms_migration_pairs_v2 ({', '.join(cms_v2_columns)}) values ({placeholders})",
            ([row[column] for column in cms_v2_columns] for row in cms_migration_pairs_v2),
        )

    connection.execute(
        """
        create table cms_stability as
        select
            leads.root_domain,
            min(case when tt.has_current_detected = 1 then tt.first_detected end) as oldest_current_first_detected,
            max(case when tt.has_current_detected = 1 then tt.first_detected end) as newest_current_first_detected,
            max(
                case
                    when coalesce(cmp.migration_status, 'none') not in ('none', 'removed_only')
                    then coalesce(cmp.likely_migration_date, cmp.first_new_detected)
                end
            ) as latest_cms_change_date
        from leads
        left join technology_timelines tt on tt.root_domain = leads.root_domain
        left join cms_migration_pairs_v2 cmp on cmp.root_domain = leads.root_domain
        group by leads.root_domain
        """
    )

    connection.execute(
        """
        insert into leads_search(rowid, root_domain, company, vertical, recently_added_platforms, removed_platforms, crm_platforms, marketing_platforms, payment_platforms, hosting_providers)
        select rowid, root_domain, company, vertical, recently_added_platforms, removed_platforms, crm_platforms, marketing_platforms, payment_platforms, hosting_providers
        from leads
        """
    )

    for key, value in metadata.items():
        connection.execute("insert into metadata (key, value) values (?, ?)", (key, json.dumps(value)))

    connection.execute("create index idx_leads_country on leads(country)")
    connection.execute("create index idx_leads_priority on leads(priority_tier)")
    connection.execute("create index idx_leads_geo on leads(geo_confidence)")
    connection.execute("create index idx_leads_migration_window on leads(migration_window)")
    connection.execute("create index idx_leads_total_score on leads(total_score)")
    connection.execute("create index idx_leads_tech_spend on leads(technology_spend)")
    connection.execute("create index idx_leads_root_domain_lower on leads(root_domain)")
    connection.execute("create index idx_leads_cms_status on leads(cms_migration_status)")
    connection.execute("create index idx_leads_cms_migration_date on leads(cms_migration_likely_date)")
    connection.execute("create index idx_events_domain on platform_events(root_domain)")
    connection.execute("create index idx_events_platform on platform_events(platform)")
    connection.execute("create index idx_events_type on platform_events(event_type)")
    connection.execute("create index idx_timelines_domain on technology_timelines(root_domain)")
    connection.execute("create index idx_timelines_platform_detected on technology_timelines(platform, first_detected)")
    connection.execute("create index idx_timelines_platform_last_found on technology_timelines(platform, last_found)")
    connection.execute("create index idx_timelines_detected on technology_timelines(first_detected)")
    connection.execute("create index idx_timelines_current_detected_domain on technology_timelines(has_current_detected, first_detected, root_domain)")
    connection.execute("create index idx_migrations_domain on migration_pairs(root_domain)")
    connection.execute("create index idx_cms_v2_domain on cms_migration_pairs_v2(root_domain)")
    connection.execute("create index idx_cms_v2_status on cms_migration_pairs_v2(migration_status)")
    connection.execute("create index idx_cms_v2_recent_change on cms_migration_pairs_v2(migration_status, likely_migration_date, first_new_detected, root_domain)")
    connection.execute("create unique index idx_cms_stability_domain on cms_stability(root_domain)")
    connection.execute("create index idx_cms_stability_oldest on cms_stability(oldest_current_first_detected)")
    connection.execute("create index idx_cms_stability_newest on cms_stability(newest_current_first_detected)")
    connection.execute("create index idx_cms_stability_latest_change on cms_stability(latest_cms_change_date)")

    connection.commit()
    connection.close()


def main() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    platform_events, source_files, metadata = load_rows()
    technology_timelines = build_technology_timelines(platform_events)
    leads, migration_pairs, cms_migration_pairs_v2, overview, integrity_audit = build_leads(platform_events)
    filter_options = build_filter_options_payload(leads, platform_events, technology_timelines)

    write_csv(PROCESSED_DIR / "platform_events.csv", platform_events)
    write_csv(PROCESSED_DIR / "technology_timelines.csv", technology_timelines)
    write_csv(PROCESSED_DIR / "leads.csv", leads)
    write_csv(PROCESSED_DIR / "migration_pairs.csv", migration_pairs)
    write_csv(PROCESSED_DIR / "cms_migration_pairs_v2.csv", cms_migration_pairs_v2)

    metadata["overview"] = overview
    metadata["integrity_audit"] = integrity_audit
    metadata["source_files"] = source_files
    (PROCESSED_DIR / "summary.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    (PROCESSED_DIR / "filter_options.json").write_text(json.dumps(filter_options, indent=2), encoding="utf-8")
    (PROCESSED_DIR / "integrity_audit.json").write_text(json.dumps(integrity_audit, indent=2), encoding="utf-8")
    write_sqlite(
        PROCESSED_DIR / "builtwith.db",
        platform_events,
        technology_timelines,
        leads,
        migration_pairs,
        cms_migration_pairs_v2,
        metadata,
    )


if __name__ == "__main__":
    main()
