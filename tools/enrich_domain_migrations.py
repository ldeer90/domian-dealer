from __future__ import annotations

import csv
import json
import re
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path


ROOT = Path("/Users/laurencedeer/Desktop/BuiltWith")
PROCESSED_DIR = ROOT / "processed"
CURRENT_CUSTOM_DIR = ROOT / "BuiltWith Exports" / "Fingerprint Exports" / "Current Custom"
CURRENT_LIVE_TECH_DIR = (
    ROOT
    / "BuiltWith Exports"
    / "Fingerprint Exports"
    / "Current Live Technology Detection"
)
OLD_CUSTOM_PATH = (
    ROOT
    / "BuiltWith Exports"
    / "Fingerprint Exports"
    / "Old Redirect Uploads"
    / "Custom"
    / "old_redirect_domains_top_5000_custom_ga_ga4_gtm.csv"
)
OLD_ATTRIBUTES_PATH = (
    ROOT
    / "BuiltWith Exports"
    / "Fingerprint Exports"
    / "Old Redirect Uploads"
    / "Domain Attributes"
    / "old_redirect_domains_top_5000_domain_attributes.csv"
)
OLD_LIVE_TECH_DIR = (
    ROOT
    / "BuiltWith Exports"
    / "Fingerprint Exports"
    / "Old Redirect Uploads"
    / "Live Technology Detection"
    / "old_redirect_domains_top_5000_live_technology_detection"
)
CANDIDATES_PATH = PROCESSED_DIR / "domain_migration_candidates.csv"
DB_PATH = PROCESSED_DIR / "builtwith.db"
SECOND_LEVEL_PREFIXES = {"ac", "co", "com", "edu", "gov", "net", "org"}
HIGH_SIGNAL_TECH_SLUGS = {
    "bigcommerce",
    "cloudflare",
    "ga",
    "ga4",
    "gorgias",
    "gtm",
    "hubspot",
    "klaviyo",
    "mailchimp",
    "magento",
    "magento enterprise",
    "opencart",
    "prestashop",
    "salesforce",
    "shopify",
    "shopify hosted",
    "shopify plus",
    "woocommerce checkout",
    "wordpress",
}
LOW_SIGNAL_TECH_SLUGS = {
    "american express",
    "apple pay",
    "discover",
    "google pay",
    "mastercard",
    "paypal",
    "paypal express checkout",
    "shopify pay",
    "unionpay",
    "visa",
}
GENERIC_COMPANY_WORDS = {
    "and",
    "australia",
    "co",
    "company",
    "corp",
    "corporation",
    "group",
    "inc",
    "international",
    "limited",
    "ltd",
    "online",
    "pty",
    "store",
    "the",
}


@dataclass
class FingerprintProfile:
    root_domain: str
    company: str = ""
    country: str = ""
    state: str = ""
    city: str = ""
    emails: set[str] = field(default_factory=set)
    telephones: set[str] = field(default_factory=set)
    people: set[str] = field(default_factory=set)
    verified_profiles: set[str] = field(default_factory=set)
    ecommerce_platforms: set[str] = field(default_factory=set)
    cms_platforms: set[str] = field(default_factory=set)
    crm_platforms: set[str] = field(default_factory=set)
    marketing_platforms: set[str] = field(default_factory=set)
    payment_platforms: set[str] = field(default_factory=set)
    hosting_providers: set[str] = field(default_factory=set)
    agencies: set[str] = field(default_factory=set)
    ai_tools: set[str] = field(default_factory=set)
    technology_spend: int = 0
    sales_revenue: int = 0
    employees: int = 0
    sku: int = 0
    has_ga: bool = False
    has_ga4: bool = False
    has_gtm: bool = False
    raw_tech_names: set[str] = field(default_factory=set)
    tech_slugs: set[str] = field(default_factory=set)


@dataclass
class OldDetectionSummary:
    root_domain: str
    tech_names: set[str] = field(default_factory=set)
    tech_slugs: set[str] = field(default_factory=set)
    first_detected_any: str = ""
    last_detected_any: str = ""
    tech_count: int = 0


@dataclass
class OldAttributes:
    root_domain: str
    technology_spend: int = 0
    sales_revenue: int = 0
    social: int = 0
    employees: int = 0
    sku: int = 0
    tranco: str = ""
    page_rank: str = ""
    majestic: str = ""
    umbrella: str = ""
    overall_score: str = ""
    performance: str = ""
    accessibility: str = ""
    seo: str = ""
    best_practices: str = ""
    exclusion: str = ""
    compliance: str = ""


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
    normalized = normalise_domain(domain)
    labels = [part for part in normalized.split(".") if part]
    if len(labels) <= 2:
        return normalized
    if len(labels[-1]) == 2 and labels[-2] in SECOND_LEVEL_PREFIXES and len(labels) >= 3:
        return ".".join(labels[-3:])
    return ".".join(labels[-2:])


def extract_domain_tld(domain: str) -> str:
    root = extract_root_domain(domain)
    parts = [part for part in root.split(".") if part]
    if len(parts) <= 1:
        return ""
    if len(parts[-1]) == 2 and len(parts[-2]) <= 3:
        return ".".join(parts[-2:])
    return parts[-1]


def compute_domain_tld_relationship(current_domain: str, old_domain: str) -> str:
    current_tld = extract_domain_tld(current_domain)
    old_tld = extract_domain_tld(old_domain)
    if not current_tld or not old_tld:
        return "unknown"
    return "same_tld" if current_tld == old_tld else "cross_tld"


def parse_intish(value: str) -> int:
    cleaned = clean_text(value).replace("$", "").replace(",", "")
    if not cleaned:
        return 0
    try:
        return int(float(cleaned))
    except ValueError:
        return 0


def parse_date(value: str) -> str:
    cleaned = clean_text(value)
    if not cleaned:
        return ""
    try:
        return datetime.fromisoformat(cleaned).date().isoformat()
    except ValueError:
        return ""


def earliest_date(existing: str, candidate: str) -> str:
    candidate = parse_date(candidate)
    if not candidate:
        return existing
    if not existing:
        return candidate
    return min(existing, candidate)


def latest_date(existing: str, candidate: str) -> str:
    candidate = parse_date(candidate)
    if not candidate:
        return existing
    if not existing:
        return candidate
    return max(existing, candidate)


def midpoint_date(first_date: str, second_date: str) -> str:
    first_date = parse_date(first_date)
    second_date = parse_date(second_date)
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
    return datetime.fromtimestamp((first_ts + second_ts) / 2).date().isoformat()


def estimate_domain_migration_date(row: dict[str, str]) -> tuple[str, str]:
    redirect_first = parse_date(row.get("redirect_first_detected", ""))
    if not redirect_first:
        return "", "missing_redirect_first_seen"

    anchors = []
    for key in ("current_first_detected", "current_first_indexed"):
        value = parse_date(row.get(key, ""))
        if not value:
            continue
        try:
            gap = (datetime.fromisoformat(value).date() - datetime.fromisoformat(redirect_first).date()).days
        except ValueError:
            continue
        if 0 <= gap <= 365:
            anchors.append((value, key))

    if anchors:
        anchor_value, anchor_key = min(anchors, key=lambda item: item[0])
        return midpoint_date(redirect_first, anchor_value), f"midpoint_{anchor_key}"

    return redirect_first, "redirect_first_detected"


def split_multi(value: str) -> list[str]:
    cleaned = clean_text(value)
    if not cleaned:
        return []
    parts = re.split(r"[;|]", cleaned)
    return [part.strip() for part in parts if part.strip()]


def tech_slug(value: str) -> str:
    text = clean_text(value).lower()
    text = re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", text)).strip()
    if not text:
        return ""
    if "google analytics 4" in text:
        return "ga4"
    if "google analytics" in text:
        return "ga"
    if "google tag manager" in text:
        return "gtm"
    if "shopify plus" in text:
        return "shopify plus"
    if "shopify hosted" in text:
        return "shopify hosted"
    if "shopify" in text:
        return "shopify"
    if "woocommerce" in text:
        return "woocommerce checkout"
    if "magento enterprise" in text or "adobe commerce" in text:
        return "magento enterprise"
    if "magento" in text:
        return "magento"
    if "bigcommerce" in text:
        return "bigcommerce"
    if "prestashop" in text:
        return "prestashop"
    if "opencart" in text or "ocstore" in text:
        return "opencart"
    if "maropost commerce cloud" in text or "maropost commerce" in text or "neto" in text:
        return "neto"
    if "wordpress" in text:
        return "wordpress"
    if "klaviyo" in text:
        return "klaviyo"
    if "gorgias" in text:
        return "gorgias"
    if "mailchimp" in text:
        return "mailchimp"
    if "hubspot" in text:
        return "hubspot"
    if "salesforce" in text:
        return "salesforce"
    if "cloudflare" in text:
        return "cloudflare"
    if "amazon" in text or "aws" in text:
        return "amazon"
    return text


def merge_profiles(base: FingerprintProfile, incoming: FingerprintProfile) -> FingerprintProfile:
    base.company = merge_profile_value(base.company, incoming.company)
    base.country = merge_profile_value(base.country, incoming.country)
    base.state = merge_profile_value(base.state, incoming.state)
    base.city = merge_profile_value(base.city, incoming.city)
    base.technology_spend = max(base.technology_spend, incoming.technology_spend)
    base.sales_revenue = max(base.sales_revenue, incoming.sales_revenue)
    base.employees = max(base.employees, incoming.employees)
    base.sku = max(base.sku, incoming.sku)
    base.emails.update(incoming.emails)
    base.telephones.update(incoming.telephones)
    base.people.update(incoming.people)
    base.verified_profiles.update(incoming.verified_profiles)
    base.ecommerce_platforms.update(incoming.ecommerce_platforms)
    base.cms_platforms.update(incoming.cms_platforms)
    base.crm_platforms.update(incoming.crm_platforms)
    base.marketing_platforms.update(incoming.marketing_platforms)
    base.payment_platforms.update(incoming.payment_platforms)
    base.hosting_providers.update(incoming.hosting_providers)
    base.agencies.update(incoming.agencies)
    base.ai_tools.update(incoming.ai_tools)
    base.has_ga = base.has_ga or incoming.has_ga
    base.has_ga4 = base.has_ga4 or incoming.has_ga4
    base.has_gtm = base.has_gtm or incoming.has_gtm
    base.raw_tech_names.update(incoming.raw_tech_names)
    base.tech_slugs.update(incoming.tech_slugs)
    return base


def load_current_profiles_from_db() -> dict[str, FingerprintProfile]:
    profiles: dict[str, FingerprintProfile] = {}
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """
            select
              root_domain,
              company,
              country,
              state,
              city,
              technology_spend,
              sales_revenue,
              employees,
              sku,
              emails,
              telephones,
              people,
              verified_profiles,
              current_platforms,
              recently_added_platforms,
              removed_platforms,
              ecommerce_platforms,
              cms_platforms,
              crm_platforms,
              marketing_platforms,
              payment_platforms,
              hosting_providers,
              agencies,
              ai_tools
            from leads
            """
        ).fetchall()
    finally:
        connection.close()

    for row in rows:
        root_domain = normalise_domain(row["root_domain"])
        if not root_domain:
            continue
        profile = FingerprintProfile(root_domain=root_domain)
        profile.company = clean_text(row["company"])
        profile.country = clean_text(row["country"])
        profile.state = clean_text(row["state"])
        profile.city = clean_text(row["city"])
        profile.technology_spend = int(row["technology_spend"] or 0)
        profile.sales_revenue = int(row["sales_revenue"] or 0)
        profile.employees = int(row["employees"] or 0)
        profile.sku = int(row["sku"] or 0)
        profile.emails.update(split_multi(row["emails"] or ""))
        profile.telephones.update(split_multi(row["telephones"] or ""))
        profile.people.update(split_multi(row["people"] or ""))
        profile.verified_profiles.update(split_multi(row["verified_profiles"] or ""))
        profile.ecommerce_platforms.update(split_multi(row["ecommerce_platforms"] or ""))
        profile.cms_platforms.update(split_multi(row["cms_platforms"] or ""))
        profile.crm_platforms.update(split_multi(row["crm_platforms"] or ""))
        profile.marketing_platforms.update(split_multi(row["marketing_platforms"] or ""))
        profile.payment_platforms.update(split_multi(row["payment_platforms"] or ""))
        profile.hosting_providers.update(split_multi(row["hosting_providers"] or ""))
        profile.agencies.update(split_multi(row["agencies"] or ""))
        profile.ai_tools.update(split_multi(row["ai_tools"] or ""))
        add_tech_values(
            profile,
            split_multi(row["current_platforms"] or "")
            + split_multi(row["recently_added_platforms"] or "")
            + split_multi(row["removed_platforms"] or "")
            + list(profile.ecommerce_platforms)
            + list(profile.cms_platforms)
            + list(profile.crm_platforms)
            + list(profile.marketing_platforms)
            + list(profile.payment_platforms)
            + list(profile.hosting_providers)
            + list(profile.agencies)
            + list(profile.ai_tools),
        )
        profiles[root_domain] = profile
    return profiles


def company_slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", clean_text(value).lower())


def company_similarity(a: str, b: str) -> float:
    a_slug = company_slug(a)
    b_slug = company_slug(b)
    if not a_slug or not b_slug:
        return 0.0
    return SequenceMatcher(None, a_slug, b_slug).ratio()


def merge_profile_value(existing: str, candidate: str) -> str:
    existing = clean_text(existing)
    candidate = clean_text(candidate)
    if not existing:
        return candidate
    if not candidate:
        return existing
    return existing if len(existing) >= len(candidate) else candidate


def add_tech_values(profile: FingerprintProfile, values: list[str]) -> None:
    for value in values:
        cleaned = clean_text(value)
        if not cleaned:
            continue
        profile.raw_tech_names.add(cleaned)
        slug = tech_slug(cleaned)
        if slug:
            profile.tech_slugs.add(slug)


def load_current_profiles() -> dict[str, FingerprintProfile]:
    profiles = load_current_profiles_from_db()
    for path in sorted(CURRENT_CUSTOM_DIR.glob("*.csv")):
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                root_domain = normalise_domain(row.get("Root Domain", ""))
                if not root_domain:
                    continue
                profile = profiles.setdefault(root_domain, FingerprintProfile(root_domain=root_domain))
                incoming = FingerprintProfile(root_domain=root_domain)
                incoming.company = clean_text(row.get("Company", ""))
                incoming.country = clean_text(row.get("Country", ""))
                incoming.state = clean_text(row.get("State", ""))
                incoming.city = clean_text(row.get("City", ""))
                incoming.technology_spend = parse_intish(row.get("Technology Spend", ""))
                incoming.sales_revenue = parse_intish(row.get("Sales Revenue", ""))
                incoming.employees = parse_intish(row.get("Employees", ""))
                incoming.sku = parse_intish(row.get("SKU", ""))
                incoming.emails.update(split_multi(row.get("Emails", "")))
                incoming.telephones.update(split_multi(row.get("Telephones", "")))
                incoming.people.update(split_multi(row.get("People", "")))
                incoming.verified_profiles.update(split_multi(row.get("Verified Profiles", "")))
                incoming.ecommerce_platforms.update(split_multi(row.get("eCommerce Platform", "")))
                incoming.cms_platforms.update(split_multi(row.get("CMS Platform", "")))
                incoming.crm_platforms.update(split_multi(row.get("CRM Platform", "")))
                incoming.marketing_platforms.update(split_multi(row.get("Marketing Automation Platform", "")))
                incoming.payment_platforms.update(split_multi(row.get("Payment Platforms", "")))
                incoming.hosting_providers.update(split_multi(row.get("Hosting Provider", "")))
                incoming.agencies.update(split_multi(row.get("Agency", "")))
                incoming.ai_tools.update(split_multi(row.get("AI", "")))
                incoming.has_ga = clean_text(row.get("Google Analytics", "")).lower() == "yes"
                incoming.has_ga4 = clean_text(row.get("Google Analytics 4", "")).lower() == "yes"
                incoming.has_gtm = clean_text(row.get("Google Tag Manager", "")).lower() == "yes"
                add_tech_values(
                    incoming,
                    list(incoming.ecommerce_platforms)
                    + list(incoming.cms_platforms)
                    + list(incoming.crm_platforms)
                    + list(incoming.marketing_platforms)
                    + list(incoming.payment_platforms)
                    + list(incoming.hosting_providers)
                    + list(incoming.agencies)
                    + list(incoming.ai_tools),
                )
                if incoming.has_ga:
                    incoming.tech_slugs.add("ga")
                if incoming.has_ga4:
                    incoming.tech_slugs.add("ga4")
                if incoming.has_gtm:
                    incoming.tech_slugs.add("gtm")
                merge_profiles(profile, incoming)

    for path in sorted(CURRENT_LIVE_TECH_DIR.glob("*.csv")):
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                root_domain = normalise_domain(row.get("Domain", ""))
                if not root_domain:
                    continue
                profile = profiles.setdefault(root_domain, FingerprintProfile(root_domain=root_domain))
                tech_name = clean_text(row.get("Technology Name", ""))
                if not tech_name:
                    continue
                profile.raw_tech_names.add(tech_name)
                slug = tech_slug(tech_name)
                if slug:
                    profile.tech_slugs.add(slug)
                lowered = tech_name.lower()
                if "google analytics 4" in lowered:
                    profile.has_ga4 = True
                    profile.tech_slugs.add("ga4")
                elif "google analytics" in lowered:
                    profile.has_ga = True
                    profile.tech_slugs.add("ga")
                elif lowered == "google tag manager":
                    profile.has_gtm = True
                    profile.tech_slugs.add("gtm")
    return profiles


def load_old_custom_profiles() -> dict[str, FingerprintProfile]:
    profiles: dict[str, FingerprintProfile] = {}
    with OLD_CUSTOM_PATH.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            root_domain = normalise_domain(row.get("Root Domain", ""))
            if not root_domain:
                continue
            profile = profiles.setdefault(root_domain, FingerprintProfile(root_domain=root_domain))
            profile.company = merge_profile_value(profile.company, row.get("Company", ""))
            profile.country = merge_profile_value(profile.country, row.get("Country", ""))
            profile.state = merge_profile_value(profile.state, row.get("State", ""))
            profile.city = merge_profile_value(profile.city, row.get("City", ""))
            profile.technology_spend = max(profile.technology_spend, parse_intish(row.get("Technology Spend", "")))
            profile.sales_revenue = max(profile.sales_revenue, parse_intish(row.get("Sales Revenue", "")))
            profile.employees = max(profile.employees, parse_intish(row.get("Employees", "")))
            profile.sku = max(profile.sku, parse_intish(row.get("SKU", "")))
            profile.emails.update(split_multi(row.get("Emails", "")))
            profile.telephones.update(split_multi(row.get("Telephones", "")))
            profile.people.update(split_multi(row.get("People", "")))
            profile.verified_profiles.update(split_multi(row.get("Verified Profiles", "")))
            profile.ecommerce_platforms.update(split_multi(row.get("eCommerce Platform", "")))
            profile.cms_platforms.update(split_multi(row.get("CMS Platform", "")))
            profile.crm_platforms.update(split_multi(row.get("CRM Platform", "")))
            profile.marketing_platforms.update(split_multi(row.get("Marketing Automation Platform", "")))
            profile.payment_platforms.update(split_multi(row.get("Payment Platforms", "")))
            profile.hosting_providers.update(split_multi(row.get("Hosting Provider", "")))
            profile.agencies.update(split_multi(row.get("Agency", "")))
            profile.ai_tools.update(split_multi(row.get("AI", "")))
            profile.has_ga = profile.has_ga or clean_text(row.get("Google Analytics", "")).lower() == "yes"
            profile.has_ga4 = profile.has_ga4 or clean_text(row.get("Google Analytics 4", "")).lower() == "yes"
            profile.has_gtm = profile.has_gtm or clean_text(row.get("Google Tag Manager", "")).lower() == "yes"
            add_tech_values(
                profile,
                list(profile.ecommerce_platforms)
                + list(profile.cms_platforms)
                + list(profile.crm_platforms)
                + list(profile.marketing_platforms)
                + list(profile.payment_platforms)
                + list(profile.hosting_providers)
                + list(profile.agencies)
                + list(profile.ai_tools),
            )
            if profile.has_ga:
                profile.tech_slugs.add("ga")
            if profile.has_ga4:
                profile.tech_slugs.add("ga4")
            if profile.has_gtm:
                profile.tech_slugs.add("gtm")
    return profiles


def load_old_attributes() -> dict[str, OldAttributes]:
    attributes: dict[str, OldAttributes] = {}
    with OLD_ATTRIBUTES_PATH.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            root_domain = normalise_domain(row.get("Root Domain", ""))
            if not root_domain:
                continue
            attributes[root_domain] = OldAttributes(
                root_domain=root_domain,
                technology_spend=parse_intish(row.get("Technology Spend", "")),
                sales_revenue=parse_intish(row.get("Sales Revenue", "")),
                social=parse_intish(row.get("Social", "")),
                employees=parse_intish(row.get("Employees", "")),
                sku=parse_intish(row.get("SKU", "")),
                tranco=clean_text(row.get("Tranco", "")),
                page_rank=clean_text(row.get("Page Rank", "")),
                majestic=clean_text(row.get("Majestic", "")),
                umbrella=clean_text(row.get("Umbrella", "")),
                overall_score=clean_text(row.get("Overall Score", "")),
                performance=clean_text(row.get("Performance", "")),
                accessibility=clean_text(row.get("Accessibility", "")),
                seo=clean_text(row.get("SEO", "")),
                best_practices=clean_text(row.get("Best Practices", "")),
                exclusion=clean_text(row.get("Exclusion", "")),
                compliance=clean_text(row.get("Compliance", "")),
            )
    return attributes


def load_old_live_detection() -> dict[str, OldDetectionSummary]:
    summaries: dict[str, OldDetectionSummary] = {}
    for path in sorted(OLD_LIVE_TECH_DIR.glob("*.csv")):
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                root_domain = normalise_domain(row.get("Domain", ""))
                if not root_domain:
                    continue
                summary = summaries.setdefault(root_domain, OldDetectionSummary(root_domain=root_domain))
                tech_name = clean_text(row.get("Technology Name", ""))
                if tech_name:
                    summary.tech_names.add(tech_name)
                    slug = tech_slug(tech_name)
                    if slug:
                        summary.tech_slugs.add(slug)
                summary.first_detected_any = earliest_date(summary.first_detected_any, row.get("First Detected", ""))
                summary.last_detected_any = latest_date(summary.last_detected_any, row.get("Last Detected", ""))
    for summary in summaries.values():
        summary.tech_count = len(summary.tech_names)
    return summaries


def ratio_similarity(a: int, b: int) -> bool:
    if not a or not b:
        return False
    high = max(a, b)
    low = min(a, b)
    return low > 0 and high / low <= 3


def fingerprint_band(score: int) -> str:
    if score >= 14:
        return "Strong"
    if score >= 8:
        return "Moderate"
    if score >= 4:
        return "Weak"
    return "None"


def confidence_band(score: int) -> str:
    if score >= 80:
        return "High"
    if score >= 60:
        return "Medium"
    return "Low"


def replace_sqlite_table(connection: sqlite3.Connection, table_name: str, rows: list[dict[str, str]]) -> None:
    connection.execute(f"drop table if exists {table_name}")
    if not rows:
        return
    columns = list(rows[0].keys())
    connection.execute(f"create table {table_name} ({', '.join(f'{column} text' for column in columns)})")
    placeholders = ", ".join("?" for _ in columns)
    connection.executemany(
        f"insert into {table_name} ({', '.join(columns)}) values ({placeholders})",
        ([row[column] for column in columns] for row in rows),
    )


def enrich_candidates(
    current_profiles: dict[str, FingerprintProfile],
    old_profiles: dict[str, FingerprintProfile],
    old_attributes: dict[str, OldAttributes],
    old_detection: dict[str, OldDetectionSummary],
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with CANDIDATES_PATH.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            current_domain = normalise_domain(row.get("current_domain", ""))
            old_domain = normalise_domain(row.get("old_domain", ""))
            current_profile = current_profiles.get(current_domain)
            old_profile = old_profiles.get(old_domain)
            attr_profile = old_attributes.get(old_domain)
            detection_profile = old_detection.get(old_domain)

            fingerprint_score_value = 0
            fingerprint_penalty = 0
            shared_signals: list[str] = []
            shared_technologies: list[str] = []
            notes: list[str] = []

            if current_profile:
                notes.append("current custom profile present")
            if old_profile:
                notes.append("old custom profile present")
            if attr_profile:
                notes.append("old domain attributes present")
            if detection_profile:
                notes.append("old live detection present")

            current_tech_slugs = set(current_profile.tech_slugs) if current_profile else set()
            old_tech_slugs = set()
            if old_profile:
                old_tech_slugs |= old_profile.tech_slugs
            if detection_profile:
                old_tech_slugs |= detection_profile.tech_slugs

            shared_high_signal = sorted((current_tech_slugs & old_tech_slugs & HIGH_SIGNAL_TECH_SLUGS) - LOW_SIGNAL_TECH_SLUGS)
            if shared_high_signal:
                shared_technologies.extend(shared_high_signal)
                if any(token in shared_high_signal for token in {"shopify", "shopify plus", "woocommerce checkout", "bigcommerce", "magento", "magento enterprise", "prestashop", "opencart"}):
                    fingerprint_score_value += 6
                    shared_signals.append("shared_platform_family")
                fingerprint_score_value += min(6, len(shared_high_signal) * 2)
                shared_signals.append(f"shared_high_signal_tech:{','.join(shared_high_signal[:5])}")

            if current_profile:
                if current_profile.has_ga and ((old_profile and old_profile.has_ga) or "ga" in old_tech_slugs):
                    fingerprint_score_value += 3
                    shared_signals.append("shared_ga")
                if current_profile.has_ga4 and ((old_profile and old_profile.has_ga4) or "ga4" in old_tech_slugs):
                    fingerprint_score_value += 4
                    shared_signals.append("shared_ga4")
                if current_profile.has_gtm and ((old_profile and old_profile.has_gtm) or "gtm" in old_tech_slugs):
                    fingerprint_score_value += 4
                    shared_signals.append("shared_gtm")

            if current_profile and old_profile:
                sim = company_similarity(current_profile.company or row.get("current_company", ""), old_profile.company)
                if sim >= 0.9:
                    fingerprint_score_value += 8
                    shared_signals.append("company_similarity_high")
                elif sim >= 0.75:
                    fingerprint_score_value += 6
                    shared_signals.append("company_similarity_good")
                elif sim >= 0.6:
                    fingerprint_score_value += 4
                    shared_signals.append("company_similarity_partial")
                elif old_profile.company and sim <= 0.35 and not shared_high_signal:
                    fingerprint_penalty += 4
                    notes.append("old company differs from current company")

                if current_profile.country and old_profile.country:
                    if current_profile.country == old_profile.country:
                        fingerprint_score_value += 3
                        shared_signals.append("country_match")
                    else:
                        fingerprint_penalty += 4
                        notes.append("old country differs from current country")
                if current_profile.state and old_profile.state and current_profile.state == old_profile.state:
                    fingerprint_score_value += 2
                    shared_signals.append("state_match")
                if current_profile.city and old_profile.city and current_profile.city == old_profile.city:
                    fingerprint_score_value += 2
                    shared_signals.append("city_match")

                shared_emails = current_profile.emails & old_profile.emails
                shared_phones = current_profile.telephones & old_profile.telephones
                if shared_emails:
                    fingerprint_score_value += 5
                    shared_signals.append("shared_email")
                    notes.append(f"shared emails: {', '.join(sorted(shared_emails)[:2])}")
                if shared_phones:
                    fingerprint_score_value += 5
                    shared_signals.append("shared_phone")
                    notes.append(f"shared phones: {', '.join(sorted(shared_phones)[:2])}")

            if current_profile and attr_profile:
                if ratio_similarity(current_profile.technology_spend, attr_profile.technology_spend):
                    fingerprint_score_value += 2
                    shared_signals.append("spend_band_match")
                if ratio_similarity(current_profile.sales_revenue, attr_profile.sales_revenue):
                    fingerprint_score_value += 2
                    shared_signals.append("revenue_band_match")

            if detection_profile and detection_profile.tech_count:
                notes.append(f"old domain has {detection_profile.tech_count} detected technologies")

            net_fingerprint_score = max(0, fingerprint_score_value - fingerprint_penalty)
            fingerprint_strength = fingerprint_band(net_fingerprint_score)
            base_score = int(row.get("confidence_score", "0") or 0)
            enhanced_confidence_score = max(0, min(100, base_score + net_fingerprint_score))
            enhanced_band = confidence_band(enhanced_confidence_score)
            estimated_migration_date, migration_date_source = estimate_domain_migration_date(row)

            enriched = dict(row)
            enriched.update(
                {
                    "current_custom_profile_present": "1" if current_profile else "0",
                    "old_custom_profile_present": "1" if old_profile else "0",
                    "old_domain_attributes_present": "1" if attr_profile else "0",
                    "old_live_detection_present": "1" if detection_profile else "0",
                    "old_company": old_profile.company if old_profile else "",
                    "old_country": old_profile.country if old_profile else "",
                    "old_state": old_profile.state if old_profile else "",
                    "old_city": old_profile.city if old_profile else "",
                    "old_ecommerce_platforms": " | ".join(sorted(old_profile.ecommerce_platforms)) if old_profile else "",
                    "old_cms_platforms": " | ".join(sorted(old_profile.cms_platforms)) if old_profile else "",
                    "old_hosting_providers": " | ".join(sorted(old_profile.hosting_providers)) if old_profile else "",
                    "old_has_ga": "1" if old_profile and old_profile.has_ga else "0",
                    "old_has_ga4": "1" if old_profile and old_profile.has_ga4 else "0",
                    "old_has_gtm": "1" if old_profile and old_profile.has_gtm else "0",
                    "old_detection_tech_count": str(detection_profile.tech_count if detection_profile else 0),
                    "old_detection_first_detected_any": detection_profile.first_detected_any if detection_profile else "",
                    "old_detection_last_detected_any": detection_profile.last_detected_any if detection_profile else "",
                    "shared_high_signal_technologies": " | ".join(shared_technologies),
                    "shared_signal_flags": " | ".join(shared_signals),
                    "fingerprint_score": str(net_fingerprint_score),
                    "fingerprint_penalty": str(fingerprint_penalty),
                    "fingerprint_strength": fingerprint_strength,
                    "enhanced_confidence_score": str(enhanced_confidence_score),
                    "enhanced_confidence_band": enhanced_band,
                    "domain_migration_estimated_date": estimated_migration_date,
                    "domain_redirect_first_seen": parse_date(row.get("redirect_first_detected", "")),
                    "domain_redirect_last_seen": parse_date(row.get("redirect_last_detected", "")),
                    "domain_migration_date_source": migration_date_source,
                    "fingerprint_notes": " | ".join(notes),
                }
            )
            rows.append(enriched)

    rows.sort(
        key=lambda item: (
            -int(item["enhanced_confidence_score"] or 0),
            -int(item["fingerprint_score"] or 0),
            -int(item["confidence_score"] or 0),
            item["current_domain"],
            item["old_domain"],
        )
    )
    return rows


def build_enriched_best_matches(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[row["current_domain"]].append(row)

    best_rows: list[dict[str, str]] = []
    for current_domain, candidates in grouped.items():
        best = max(
            candidates,
            key=lambda item: (
                int(item["enhanced_confidence_score"] or 0),
                int(item["fingerprint_score"] or 0),
                int(item["confidence_score"] or 0),
                int(item["brand_similarity_score"] or 0),
                int(item["redirect_duration_days"] or 0),
            ),
        )
        best_rows.append(
            {
                "current_domain": current_domain,
                "best_old_domain": best["old_domain"],
                "domain_migration_estimated_date": best["domain_migration_estimated_date"],
                "domain_redirect_first_seen": best["domain_redirect_first_seen"],
                "domain_redirect_last_seen": best["domain_redirect_last_seen"],
                "domain_migration_date_source": best["domain_migration_date_source"],
                "enhanced_confidence_score": best["enhanced_confidence_score"],
                "enhanced_confidence_band": best["enhanced_confidence_band"],
                "base_confidence_score": best["confidence_score"],
                "base_confidence_band": best["confidence_band"],
                "fingerprint_score": best["fingerprint_score"],
                "fingerprint_strength": best["fingerprint_strength"],
                "number_of_old_domains_found": best["number_of_old_domains_for_current"],
                "current_company": best["current_company"],
                "country": best["country"],
                "current_priority_tier": best["current_priority_tier"],
                "shared_high_signal_technologies": best["shared_high_signal_technologies"],
                "shared_signal_flags": best["shared_signal_flags"],
                "old_company": best["old_company"],
                "old_country": best["old_country"],
                "old_ecommerce_platforms": best["old_ecommerce_platforms"],
                "old_detection_tech_count": best["old_detection_tech_count"],
                "notes": best["notes"],
                "fingerprint_notes": best["fingerprint_notes"],
            }
        )

    best_rows.sort(
        key=lambda item: (
            -int(item["enhanced_confidence_score"] or 0),
            -int(item["fingerprint_score"] or 0),
            item["current_domain"],
        )
    )
    return best_rows


def build_ui_best_matches(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    domain_pairs = {(row["current_domain"], row["old_domain"]) for row in rows}
    for row in rows:
        grouped[row["current_domain"]].append(row)

    ui_rows: list[dict[str, str]] = []
    for current_domain, candidates in grouped.items():
        best = max(
            candidates,
            key=lambda item: (
                6 if item["fingerprint_strength"] in {"Strong", "Moderate"} else 0,
                5
                if compute_domain_tld_relationship(item["current_domain"], item["old_domain"]) == "same_tld"
                and int(item["enhanced_confidence_score"] or 0) >= 60
                else 0,
                4 if item["shared_signal_flags"] or item["shared_high_signal_technologies"] else 0,
                3 if item["fingerprint_strength"] == "Weak" else 0,
                2 if int(item["enhanced_confidence_score"] or 0) >= 80 else 0,
                2 if compute_domain_tld_relationship(item["current_domain"], item["old_domain"]) == "same_tld" else 1 if compute_domain_tld_relationship(item["current_domain"], item["old_domain"]) == "cross_tld" else 0,
                int(item["enhanced_confidence_score"] or 0),
                int(item["fingerprint_score"] or 0),
                int(item["redirect_duration_days"] or 0),
            ),
        )
        relationship = compute_domain_tld_relationship(best["current_domain"], best["old_domain"])
        has_reverse = (best["old_domain"], best["current_domain"]) in domain_pairs
        notes_lower = (best["notes"] or "").lower()
        if "unrelated" in notes_lower or "weak domain similarity" in notes_lower:
            status = "network" if has_reverse else "weak"
        elif best["fingerprint_strength"] in {"Strong", "Moderate"}:
            status = "confirmed"
        elif has_reverse:
            status = "network"
        elif best["shared_signal_flags"] or best["shared_high_signal_technologies"] or best["fingerprint_strength"] == "Weak" or relationship == "same_tld":
            status = "probable"
        else:
            status = "weak"

        if best["fingerprint_strength"] in {"Strong", "Moderate"}:
            reason = "Redirect evidence is supported by shared business fingerprints."
        elif has_reverse:
            reason = "Domains appear to be part of a redirect cluster or canonical handover."
        elif relationship == "same_tld":
            reason = "Redirect evidence points to a likely same-market domain move, but fingerprint support is lighter."
        else:
            reason = "Redirect evidence exists, but stronger business fingerprint support is still needed."

        warning_flags: list[str] = []
        if has_reverse:
            warning_flags.append("bidirectional_redirect_history")
        if best["fingerprint_strength"] in {"", "None"}:
            warning_flags.append("fingerprint_unavailable")
        if "unrelated" in notes_lower or "weak domain similarity" in notes_lower:
            warning_flags.append("redirect_only_match")

        evidence_flags = " | ".join(
            item
            for item in [
                best["shared_signal_flags"],
                best["shared_high_signal_technologies"],
            ]
            if item
        )

        ui_rows.append(
            {
                "current_domain": current_domain,
                "best_old_domain": best["old_domain"],
                "domain_migration_estimated_date": best["domain_migration_estimated_date"],
                "domain_redirect_first_seen": best["domain_redirect_first_seen"],
                "domain_redirect_last_seen": best["domain_redirect_last_seen"],
                "domain_migration_date_source": best["domain_migration_date_source"],
                "domain_migration_status": status,
                "domain_migration_reason": reason,
                "domain_migration_confidence_score": best["enhanced_confidence_score"],
                "domain_migration_confidence_band": best["enhanced_confidence_band"],
                "domain_fingerprint_strength": best["fingerprint_strength"],
                "domain_migration_candidate_count": best["number_of_old_domains_for_current"],
                "domain_shared_signals": best["shared_signal_flags"],
                "domain_shared_technologies": best["shared_high_signal_technologies"],
                "domain_migration_notes": best["notes"],
                "domain_fingerprint_notes": best["fingerprint_notes"],
                "old_company": best["old_company"],
                "old_country": best["old_country"],
                "old_ecommerce_platforms": best["old_ecommerce_platforms"],
                "domain_tld_relationship": relationship,
                "domain_migration_warning_flags": " | ".join(warning_flags),
                "domain_migration_evidence_flags": evidence_flags,
            }
        )

    ui_rows.sort(key=lambda item: (-int(item["domain_migration_confidence_score"] or 0), item["current_domain"]))
    return ui_rows


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def export_outputs(
    rows: list[dict[str, str]],
    best_rows: list[dict[str, str]],
    ui_best_rows: list[dict[str, str]],
    metadata: dict[str, object],
) -> None:
    write_csv(PROCESSED_DIR / "domain_migration_candidates_enriched.csv", rows)
    write_csv(PROCESSED_DIR / "domain_migration_best_match_enriched.csv", best_rows)
    write_csv(PROCESSED_DIR / "domain_migration_best_match_ui.csv", ui_best_rows)
    (PROCESSED_DIR / "domain_migration_fingerprint_summary.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")

    connection = sqlite3.connect(DB_PATH)
    try:
        replace_sqlite_table(connection, "domain_migration_candidates_enriched", rows)
        replace_sqlite_table(connection, "domain_migration_best_match_enriched", best_rows)
        replace_sqlite_table(connection, "domain_migration_best_match_ui", ui_best_rows)
        connection.execute("create index if not exists idx_domain_migration_enriched_current on domain_migration_candidates_enriched(current_domain)")
        connection.execute("create index if not exists idx_domain_migration_enriched_old on domain_migration_candidates_enriched(old_domain)")
        connection.execute("create index if not exists idx_domain_migration_best_enriched_current on domain_migration_best_match_enriched(current_domain)")
        connection.execute("create index if not exists idx_domain_migration_best_ui_current on domain_migration_best_match_ui(current_domain)")
        connection.execute(
            "create index if not exists idx_domain_migration_best_ui_estimated_date on domain_migration_best_match_ui(domain_migration_estimated_date)"
        )
        connection.commit()
    finally:
        connection.close()


def main() -> None:
    current_profiles = load_current_profiles()
    old_profiles = load_old_custom_profiles()
    old_attributes = load_old_attributes()
    old_detection = load_old_live_detection()

    rows = enrich_candidates(current_profiles, old_profiles, old_attributes, old_detection)
    best_rows = build_enriched_best_matches(rows)
    ui_best_rows = build_ui_best_matches(rows)

    metadata = {
        "processed_at": datetime.now().isoformat(),
        "current_custom_profile_count": len(current_profiles),
        "old_custom_profile_count": len(old_profiles),
        "old_attributes_count": len(old_attributes),
        "old_live_detection_domain_count": len(old_detection),
        "enriched_candidate_count": len(rows),
        "enriched_best_match_count": len(best_rows),
        "fingerprint_strength_counts": dict(Counter(row["fingerprint_strength"] for row in rows)),
        "enhanced_confidence_band_counts": dict(Counter(row["enhanced_confidence_band"] for row in rows)),
        "best_match_fingerprint_strength_counts": dict(Counter(row["fingerprint_strength"] for row in best_rows)),
        "best_match_enhanced_confidence_band_counts": dict(Counter(row["enhanced_confidence_band"] for row in best_rows)),
    }
    export_outputs(rows, best_rows, ui_best_rows, metadata)

    print(f"Current custom profiles: {len(current_profiles)}")
    print(f"Old custom profiles: {len(old_profiles)}")
    print(f"Old attributes profiles: {len(old_attributes)}")
    print(f"Old live detection domains: {len(old_detection)}")
    print(f"Enriched candidate rows: {len(rows)}")
    print(f"Enriched best-match rows: {len(best_rows)}")


if __name__ == "__main__":
    main()
