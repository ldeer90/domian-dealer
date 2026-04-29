import { memo, startTransition, useCallback, useDeferredValue, useEffect, useMemo, useState, type ReactNode } from "react";
import {
  addFilteredToExportTray,
  addToExportTray,
  clearExportTray,
  createPreset,
  deletePreset,
  exportLeadUrl,
  fetchExportTray,
  fetchFilterOptions,
  fetchHealth,
  fetchLeadDetail,
  fetchLeads,
  fetchPresets,
  fetchScreamingFrogJobStatus,
  fetchScreamingFrogSummary,
  fetchSeRankingSummary,
  fetchSiteStatusSummary,
  fetchSummary,
  previewManualSeRankingAnalysis,
  refreshScreamingFrogAudit,
  refreshSiteStatusCheck,
  refreshSeRankingAnalysis,
  removeFromExportTray,
  runScreamingFrogAudit,
  runSiteStatusCheck,
  runManualSeRankingAnalysis,
  runSeRankingAnalysis,
  stopScreamingFrogJobBatch,
} from "./api";
import type {
  ExportTrayResponse,
  FilterOptions,
  HealthResponse,
  Lead,
  LeadDetailResponse,
  LeadQuery,
  LeadsResponse,
  MigrationTimingOperator,
  Preset,
  ScreamingFrogJobBatch,
  ScreamingFrogRunResponse,
  ScreamingFrogSummaryResponse,
  SeRankingManualPreviewResponse,
  SeRankingSummaryResponse,
  SiteStatusSummaryResponse,
  SummaryResponse,
  TimelineDateField,
  TimelineEventType,
  TimelineGranularity,
  TimelineRow,
} from "./types";

const FILTER_SEARCH_THRESHOLD = 14;

type ColumnKey =
  | "country"
  | "vertical"
  | "current_platforms"
  | "social"
  | "sales_revenue"
  | "employees"
  | "sku"
  | "domain_migration"
  | "cms_migration"
  | "domain_fingerprint_strength"
  | "domain_shared_signals"
  | "removed_platforms"
  | "matched_timeline_platforms"
  | "matched_first_detected"
  | "matched_last_found"
  | "cms_migration_date"
  | "domain_migration_date"
  | "sales_buckets"
  | "contact_status"
  | "technology_spend"
  | "total_score"
  | "priority_tier"
  | "marketing_platforms"
  | "crm_platforms"
  | "payment_platforms"
  | "hosting_providers"
  | "agencies"
  | "ai_tools"
  | "compliance_flags"
  | "reason"
  | "se_market"
  | "se_traffic_before"
  | "se_traffic_last_month"
  | "se_traffic_change"
  | "se_keywords_before"
  | "se_keywords_last_month"
  | "se_keyword_change"
  | "se_outcome"
  | "se_checked"
  | "site_status"
  | "site_status_code"
  | "site_final_url"
  | "site_checked"
  | "sf_status"
  | "sf_config"
  | "sf_quality"
  | "sf_score"
  | "sf_primary_issue"
  | "sf_checked"
  | "sf_pages_crawled"
  | "sf_homepage_status"
  | "sf_title_issues"
  | "sf_meta_issues"
  | "sf_canonical_issues"
  | "sf_internal_errors"
  | "sf_location_pages"
  | "sf_service_pages"
  | "sf_strengths"
  | "sf_issue_signals"
  | "sf_heading_health"
  | "sf_evidence"
  | "sf_collection_detection"
  | "sf_collection_intro"
  | "sf_collection_snippet"
  | "sf_collection_title_signal"
  | "sf_collection_confidence"
  | "sf_title_optimization"
  | "sf_collection_products"
  | "sf_collection_schema";

type SeRankingAnalysisType = "cms_migration" | "domain_migration";
type ScreamingFrogCrawlMode = "bounded_audit" | "deep_audit";
type InlineRunStatus = {
  tone: "neutral" | "positive" | "warning";
  label: string;
  message: string;
} | null;

type BackendConnectionState = "checking" | "connected" | "degraded" | "offline";
type LeadTableState = "loading" | "ready" | "empty" | "retrying" | "error";

const BACKEND_APP_BASE = (((import.meta.env.VITE_API_BASE as string | undefined) ?? "http://127.0.0.1:8765/api").replace(/\/api\/?$/, ""));

const DEFAULT_TIMELINE_EVENT_TYPES: TimelineEventType[] = ["current_detected", "recently_added"];
const TIMELINE_EVENT_LABELS: Record<TimelineEventType, string> = {
  current_detected: "Current install",
  recently_added: "Recently added",
  no_longer_detected: "No longer detected",
};

const FRIENDLY_LABELS: Record<string, string> = {
  AU: "Australia",
  NZ: "New Zealand",
  SG: "Singapore",
  ab: "A/B",
  ai: "AI",
  api: "API",
  cms: "CMS",
  crm: "CRM",
  geo_review_needed: "Needs geo review",
  shopify: "Shopify",
  shopify_plus: "Shopify Plus",
  woocommerce: "WooCommerce",
  woocommerce_checkout: "WooCommerce Checkout",
  bigcommerce: "BigCommerce",
  wordpress: "WordPress",
  wix: "Wix",
  squarespace: "Squarespace",
  webflow: "Webflow",
  drupal: "Drupal",
  joomla: "Joomla",
  duda: "Duda",
  craft: "Craft CMS",
  umbraco: "Umbraco",
  framer: "Framer",
  opencart: "OpenCart",
  prestashop: "PrestaShop",
  magento: "Magento",
  magento_enterprise: "Magento Enterprise",
  neto: "Neto",
  current_shopify_removed_woocommerce: "Current Shopify after WooCommerce",
  current_shopify_plus_high_spend: "Current Shopify Plus high spend",
  current_shopify_plus_contactable: "Current Shopify Plus contactable",
  current_shopify_revenue_stack: "Current Shopify revenue stack",
  contact_ready_ab: "Contact-ready A/B",
  premium_hosting_ab: "Premium hosting A/B",
  recent_migration_signal: "Recent migration signal",
  recent_platform_adopter: "Recent platform adopter",
  switch_to_shopify: "Switch to Shopify",
  woo_to_shopify: "WooCommerce to Shopify",
  woo_to_shopify_recent: "WooCommerce to Shopify recent",
  woo_to_shopify_plus: "WooCommerce to Shopify Plus",
  woo_to_shopify_plus_recent: "WooCommerce to Shopify Plus recent",
  woo_removed_revenue_stack: "WooCommerce removed revenue stack",
  contactable_revenue_stack: "Contactable revenue stack",
  high_spend: "High spend",
  same_tld: "Same TLD",
  cross_tld: "Cross-TLD",
  unknown: "Unknown",
  confirmed: "Confirmed",
  probable: "Possible",
  possible: "Possible",
  network: "Cluster / review",
  weak: "Weak / review",
  historic: "Historic",
  overlap: "Overlap / review",
  removed_only: "Removed only",
  none: "No clear match",
  high: "High confidence",
  medium: "Medium confidence",
  low: "Low confidence",
  shared_platform_family: "Shared platform family",
  shared_ga: "Shared GA",
  shared_ga4: "Shared GA4",
  shared_gtm: "Shared Tag Manager",
  company_similarity_high: "Same company",
  country_match: "Same country",
  state_match: "Same state",
  city_match: "Same city",
  shared_phone: "Shared phone",
  shared_email: "Shared email",
  spend_band_match: "Similar tech spend",
  revenue_band_match: "Similar revenue",
  missing_recent_addition: "Missing recent-addition row",
  negative_gap: "Negative timing gap",
  current_removed_overlap_only: "Current and removed overlap only",
  conflicting_platform_snapshot: "Conflicting snapshot platform",
  fingerprint_unavailable: "No fingerprint support",
  redirect_only_match: "Redirect-only evidence",
  bidirectional_redirect_history: "Bidirectional redirect history",
  current_recent_mismatch: "Current and recent platform mismatch",
  current_platform_seen: "Current platform seen",
  recent_addition_seen: "Recent addition seen",
  removed_platform_seen: "Previous platform seen",
  tight_timing: "Tight timing window",
  current_detected: "Current install",
  recently_added: "Recently added",
  no_longer_detected: "No longer detected",
  cms_migration: "CMS migration",
  domain_migration: "Domain migration",
  traffic_up: "Traffic up",
  traffic_down: "Traffic down",
  traffic_flat: "Traffic flat",
  traffic_up_20_plus: "Traffic up 20%+",
  traffic_down_20_plus: "Traffic down 20%+",
  traffic_up_50_plus: "Traffic up 50%+",
  keywords_up: "Keywords up",
  keywords_down: "Keywords down",
  keywords_flat: "Keywords flat",
  keywords_up_20_plus: "Keywords up 20%+",
  keywords_down_20_plus: "Keywords down 20%+",
  keywords_up_50_plus: "Keywords up 50%+",
  success: "Available",
  partial: "Partial",
  error: "Error",
  ok: "Live",
  redirect: "Redirecting",
  not_found: "404",
  server_error: "Server error",
  blocked: "Blocked",
  timeout: "Timeout",
  dns_error: "DNS error",
  ssl_error: "SSL error",
  other_error: "Other error",
  bounded_audit: "Bounded audit",
  deep_audit: "Deep audit",
  client_error: "Client error",
  missing: "Missing",
  duplicate: "Duplicate",
  too_long: "Too long",
  too_short: "Too short",
  non_indexable: "Non-indexable",
  inconsistent: "Inconsistent",
};

const PLATFORM_COLOURS: Record<string, string> = {
  shopify: "#155eef",
  shopify_plus: "#1d4ed8",
  woocommerce_checkout: "#7c3aed",
  bigcommerce: "#0f7b6c",
  wordpress: "#21759b",
  wix: "#6c5ce7",
  squarespace: "#111827",
  webflow: "#2563eb",
  drupal: "#0b6cad",
  joomla: "#f57c00",
  duda: "#7c3aed",
  craft: "#e5422b",
  umbraco: "#3544b1",
  framer: "#111111",
  opencart: "#b54708",
  prestashop: "#c11574",
  magento: "#d92d20",
  magento_enterprise: "#9e1c15",
  neto: "#2563eb",
};

const COLOUR_FALLBACKS = ["#155eef", "#0f7b6c", "#b54708", "#7c3aed", "#c11574", "#d92d20", "#1f6feb", "#087443"];

const defaultVisibleColumns: ColumnKey[] = [
  "country",
  "vertical",
  "current_platforms",
  "cms_migration",
  "cms_migration_date",
  "domain_migration",
  "domain_migration_date",
  "sales_buckets",
  "technology_spend",
  "total_score",
  "priority_tier",
  "domain_fingerprint_strength",
  "domain_shared_signals",
  "reason",
  "contact_status",
];

const MIGRATION_DATE_PRESETS = [
  { label: "3m", months: 3 },
  { label: "6m", months: 6 },
  { label: "12m", months: 12 },
  { label: "23m", months: 23 },
] as const;

const initialQuery: LeadQuery = {
  search: "",
  exactDomain: "",
  countries: ["AU", "NZ", "SG"],
  tiers: ["A", "B", "C", "D"],
  currentPlatforms: [],
  recentPlatforms: [],
  removedPlatforms: [],
  verticals: [],
  salesBuckets: [],
  liveSitesOnly: false,
  migrationOnly: false,
  hasDomainMigration: false,
  hasCmsMigration: false,
  domainMigrationStatuses: [],
  domainConfidenceBands: [],
  domainFingerprintStrengths: [],
  domainTldRelationships: [],
  cmsMigrationStatuses: [],
  cmsConfidenceLevels: [],
  hasContact: false,
  hasMarketing: false,
  hasCrm: false,
  hasPayments: false,
  marketingPlatforms: [],
  crmPlatforms: [],
  paymentPlatforms: [],
  hostingProviders: [],
  agencies: [],
  aiTools: [],
  complianceFlags: [],
  minSocial: "",
  minRevenue: "",
  minEmployees: "",
  minSku: "",
  minTechnologySpend: "",
  selectedOnly: false,
  hasSeRankingAnalysis: false,
  seRankingAnalysisTypes: [],
  seRankingOutcomeFlags: [],
  hasSiteStatusCheck: false,
  siteStatusCategories: [],
  hasScreamingFrogAudit: false,
  screamingFrogStatuses: [],
  screamingFrogHomepageStatuses: [],
  screamingFrogTitleFlags: [],
  screamingFrogMetaFlags: [],
  screamingFrogCanonicalFlags: [],
  hasScreamingFrogInternalErrors: false,
  hasScreamingFrogLocationPages: false,
  hasScreamingFrogServicePages: false,
  timelinePlatforms: [],
  timelineEventTypes: DEFAULT_TIMELINE_EVENT_TYPES,
  timelineDateField: "first_seen",
  timelineSeenFrom: "",
  timelineSeenTo: "",
  cmsMigrationFrom: "",
  cmsMigrationTo: "",
  cmsUnchangedYears: "",
  domainMigrationFrom: "",
  domainMigrationTo: "",
  migrationTimingOperator: "and",
  timelineGranularity: "month",
  page: 1,
  pageSize: 100,
  sortBy: "total_score",
  sortDirection: "desc",
};

const pageSizeOptions = [100, 250, 500, 1000];
const sortLabels: Record<string, string> = {
  country: "Country",
  vertical: "Vertical",
  priority_tier: "Tier",
  total_score: "Score",
  technology_spend: "Tech spend",
  contact_score: "Contactability",
  bucket_count: "Bucket count",
  domain_migration_estimated_date: "Domain migration date",
  se_ranking_checked_at: "SE checked",
  site_status_checked_at: "Site checked",
  site_status_code: "Status code",
  screamingfrog_checked_at: "SF checked",
  screamingfrog_pages_crawled: "Pages crawled",
  screamingfrog_opportunity_score: "SF score",
  matched_first_detected: "First seen",
  matched_last_found: "Last seen",
  domain_migration_status: "Previous domain",
  domain_fingerprint_strength: "Fingerprint",
  cms_migration_status: "CMS migration",
  cms_migration_likely_date: "CMS migration date",
  se_checked: "SE checked",
  company: "Company",
  root_domain: "Domain",
};

const columnLabels: Record<ColumnKey, string> = {
  country: "Country",
  vertical: "Vertical",
  current_platforms: "Current platform",
  social: "Followers",
  sales_revenue: "Revenue",
  employees: "Employees",
  sku: "SKU",
  domain_migration: "Previous domain candidate",
  cms_migration: "Possible CMS migration",
  se_market: "SE market",
  se_traffic_before: "Traffic first month",
  se_traffic_last_month: "Traffic second month",
  se_traffic_change: "Traffic change",
  se_keywords_before: "Keywords first month",
  se_keywords_last_month: "Keywords second month",
  se_keyword_change: "Keyword change",
  se_outcome: "Outcome",
  se_checked: "SE checked",
  site_status: "Site status",
  site_status_code: "Status code",
  site_final_url: "Final URL",
  site_checked: "Site checked",
  sf_status: "SF status",
  sf_config: "SF config",
  sf_quality: "SF quality",
  sf_score: "SF score",
  sf_primary_issue: "SF primary issue",
  sf_checked: "SF checked",
  sf_pages_crawled: "Pages crawled",
  sf_homepage_status: "Homepage status",
  sf_title_issues: "Title issues",
  sf_meta_issues: "Meta issues",
  sf_canonical_issues: "Canonical issues",
  sf_internal_errors: "Internal errors",
  sf_location_pages: "Location pages",
  sf_service_pages: "Service pages",
  sf_strengths: "SF strengths",
  sf_issue_signals: "SF issues",
  sf_heading_health: "Heading health",
  sf_evidence: "SF evidence",
  sf_collection_detection: "Collection detection",
  sf_collection_intro: "Collection intro",
  sf_collection_snippet: "Collection snippet",
  sf_collection_title_signal: "Collection title signal",
  sf_collection_confidence: "Collection confidence",
  sf_title_optimization: "Title optimisation",
  sf_collection_products: "Collection products",
  sf_collection_schema: "Collection schema",
  domain_fingerprint_strength: "Fingerprint",
  domain_shared_signals: "Shared signals",
  removed_platforms: "Previous platform seen",
  matched_timeline_platforms: "Timeline platform",
  matched_first_detected: "Tech first seen",
  matched_last_found: "Tech last seen",
  cms_migration_date: "CMS migration date",
  domain_migration_date: "Domain migration date",
  sales_buckets: "Lead angles",
  contact_status: "Contact",
  technology_spend: "Tech spend",
  total_score: "Score",
  priority_tier: "Tier",
  marketing_platforms: "Marketing tools",
  crm_platforms: "CRM tools",
  payment_platforms: "Payment tools",
  hosting_providers: "Hosting",
  agencies: "Agency",
  ai_tools: "AI tools",
  compliance_flags: "Compliance",
  reason: "Why this lead",
};

function toggle(values: string[], value: string) {
  return values.includes(value) ? values.filter((item) => item !== value) : [...values, value];
}

function splitPipe(value: string) {
  return value
    .split("|")
    .map((item) => item.trim())
    .filter(Boolean);
}

function useDebouncedValue<T>(value: T, delayMs: number) {
  const [debounced, setDebounced] = useState(value);

  useEffect(() => {
    const timeout = window.setTimeout(() => setDebounced(value), delayMs);
    return () => window.clearTimeout(timeout);
  }, [value, delayMs]);

  return debounced;
}

function normalizeLeadQuery(raw?: Partial<LeadQuery>): LeadQuery {
  const legacyRaw = raw as Partial<LeadQuery> & { startedFrom?: string; startedTo?: string };
  return {
    ...initialQuery,
    ...raw,
    exactDomain: raw?.exactDomain ?? initialQuery.exactDomain,
    countries: raw?.countries ?? initialQuery.countries,
    tiers: raw?.tiers ?? initialQuery.tiers,
    currentPlatforms: raw?.currentPlatforms ?? initialQuery.currentPlatforms,
    recentPlatforms: raw?.recentPlatforms ?? initialQuery.recentPlatforms,
    removedPlatforms: raw?.removedPlatforms ?? initialQuery.removedPlatforms,
    verticals: raw?.verticals ?? initialQuery.verticals,
    salesBuckets: raw?.salesBuckets ?? initialQuery.salesBuckets,
    liveSitesOnly: raw?.liveSitesOnly ?? initialQuery.liveSitesOnly,
    hasDomainMigration: raw?.hasDomainMigration ?? initialQuery.hasDomainMigration,
    hasCmsMigration: raw?.hasCmsMigration ?? initialQuery.hasCmsMigration,
    domainMigrationStatuses: raw?.domainMigrationStatuses ?? initialQuery.domainMigrationStatuses,
    domainConfidenceBands: raw?.domainConfidenceBands ?? initialQuery.domainConfidenceBands,
    domainFingerprintStrengths: raw?.domainFingerprintStrengths ?? initialQuery.domainFingerprintStrengths,
    domainTldRelationships: raw?.domainTldRelationships ?? initialQuery.domainTldRelationships,
    cmsMigrationStatuses: raw?.cmsMigrationStatuses ?? initialQuery.cmsMigrationStatuses,
    cmsConfidenceLevels: raw?.cmsConfidenceLevels ?? initialQuery.cmsConfidenceLevels,
    marketingPlatforms: raw?.marketingPlatforms ?? initialQuery.marketingPlatforms,
    crmPlatforms: raw?.crmPlatforms ?? initialQuery.crmPlatforms,
    paymentPlatforms: raw?.paymentPlatforms ?? initialQuery.paymentPlatforms,
    hostingProviders: raw?.hostingProviders ?? initialQuery.hostingProviders,
    agencies: raw?.agencies ?? initialQuery.agencies,
    aiTools: raw?.aiTools ?? initialQuery.aiTools,
    complianceFlags: raw?.complianceFlags ?? initialQuery.complianceFlags,
    minSocial: raw?.minSocial ?? initialQuery.minSocial,
    minRevenue: raw?.minRevenue ?? initialQuery.minRevenue,
    minEmployees: raw?.minEmployees ?? initialQuery.minEmployees,
    minSku: raw?.minSku ?? initialQuery.minSku,
    minTechnologySpend: raw?.minTechnologySpend ?? initialQuery.minTechnologySpend,
    hasSeRankingAnalysis: raw?.hasSeRankingAnalysis ?? initialQuery.hasSeRankingAnalysis,
    seRankingAnalysisTypes: raw?.seRankingAnalysisTypes ?? initialQuery.seRankingAnalysisTypes,
    seRankingOutcomeFlags: raw?.seRankingOutcomeFlags ?? initialQuery.seRankingOutcomeFlags,
    hasSiteStatusCheck: raw?.hasSiteStatusCheck ?? initialQuery.hasSiteStatusCheck,
    siteStatusCategories: raw?.siteStatusCategories ?? initialQuery.siteStatusCategories,
    hasScreamingFrogAudit: raw?.hasScreamingFrogAudit ?? initialQuery.hasScreamingFrogAudit,
    screamingFrogStatuses: raw?.screamingFrogStatuses ?? initialQuery.screamingFrogStatuses,
    screamingFrogHomepageStatuses: raw?.screamingFrogHomepageStatuses ?? initialQuery.screamingFrogHomepageStatuses,
    screamingFrogTitleFlags: raw?.screamingFrogTitleFlags ?? initialQuery.screamingFrogTitleFlags,
    screamingFrogMetaFlags: raw?.screamingFrogMetaFlags ?? initialQuery.screamingFrogMetaFlags,
    screamingFrogCanonicalFlags: raw?.screamingFrogCanonicalFlags ?? initialQuery.screamingFrogCanonicalFlags,
    hasScreamingFrogInternalErrors: raw?.hasScreamingFrogInternalErrors ?? initialQuery.hasScreamingFrogInternalErrors,
    hasScreamingFrogLocationPages: raw?.hasScreamingFrogLocationPages ?? initialQuery.hasScreamingFrogLocationPages,
    hasScreamingFrogServicePages: raw?.hasScreamingFrogServicePages ?? initialQuery.hasScreamingFrogServicePages,
    timelinePlatforms: raw?.timelinePlatforms ?? initialQuery.timelinePlatforms,
    timelineEventTypes: (raw?.timelineEventTypes as TimelineEventType[] | undefined) ?? initialQuery.timelineEventTypes,
    timelineDateField: (raw?.timelineDateField as TimelineDateField | undefined) ?? initialQuery.timelineDateField,
    timelineSeenFrom: raw?.timelineSeenFrom ?? legacyRaw?.startedFrom ?? initialQuery.timelineSeenFrom,
    timelineSeenTo: raw?.timelineSeenTo ?? legacyRaw?.startedTo ?? initialQuery.timelineSeenTo,
    cmsMigrationFrom: raw?.cmsMigrationFrom ?? initialQuery.cmsMigrationFrom,
    cmsMigrationTo: raw?.cmsMigrationTo ?? initialQuery.cmsMigrationTo,
    cmsUnchangedYears: raw?.cmsUnchangedYears ?? initialQuery.cmsUnchangedYears,
    domainMigrationFrom: raw?.domainMigrationFrom ?? initialQuery.domainMigrationFrom,
    domainMigrationTo: raw?.domainMigrationTo ?? initialQuery.domainMigrationTo,
    migrationTimingOperator: (raw?.migrationTimingOperator as MigrationTimingOperator | undefined) ?? initialQuery.migrationTimingOperator,
    timelineGranularity: (raw?.timelineGranularity as TimelineGranularity | undefined) ?? initialQuery.timelineGranularity,
    page: raw?.page ?? initialQuery.page,
    pageSize: raw?.pageSize ?? initialQuery.pageSize,
    sortBy: raw?.sortBy ?? initialQuery.sortBy,
    sortDirection: raw?.sortDirection ?? initialQuery.sortDirection,
  };
}

function ensureColumns(values: string[]) {
  const sanitized = Array.from(new Set(values.filter((value): value is ColumnKey => value in columnLabels)));
  const ordered = sanitized.length ? [...sanitized] : [...defaultVisibleColumns];
  const adjacencyPairs: Array<[ColumnKey, ColumnKey]> = [
    ["cms_migration", "cms_migration_date"],
    ["domain_migration", "domain_migration_date"],
  ];

  adjacencyPairs.forEach(([primary, companion]) => {
    const primaryIndex = ordered.indexOf(primary);
    const companionIndex = ordered.indexOf(companion);
    if (primaryIndex === -1 || companionIndex === -1) {
      return;
    }
    if (companionIndex === primaryIndex + 1) {
      return;
    }
    ordered.splice(companionIndex, 1);
    ordered.splice(primaryIndex + 1, 0, companion);
  });

  return ordered;
}

function humanizeToken(value: string): string {
  const trimmed = value.trim();
  if (!trimmed) {
    return trimmed;
  }
  if (FRIENDLY_LABELS[trimmed]) {
    return FRIENDLY_LABELS[trimmed];
  }
  if (trimmed.includes("->")) {
    return trimmed
      .split("->")
      .map((part) => humanizeToken(part))
      .join(" → ");
  }
  if (trimmed.includes("→")) {
    return trimmed
      .split("→")
      .map((part) => humanizeToken(part))
      .join(" → ");
  }
  return trimmed
    .split(/[_\s-]+/)
    .filter(Boolean)
    .map((word) => {
      const lower = word.toLowerCase();
      if (FRIENDLY_LABELS[lower]) {
        return FRIENDLY_LABELS[lower];
      }
      if (/^[A-Z]{2,}$/.test(word)) {
        return word;
      }
      return `${word.charAt(0).toUpperCase()}${word.slice(1).toLowerCase()}`;
    })
    .join(" ");
}

function describeBackendState(
  state: BackendConnectionState,
  health: HealthResponse | null,
): { label: string; tone: "neutral" | "positive" | "warning" } {
  if (state === "connected") {
    return { label: health?.worker_running ? "Backend connected · Crawl worker active" : "Backend connected", tone: "positive" };
  }
  if (state === "degraded") {
    return { label: "Backend degraded", tone: "warning" };
  }
  if (state === "offline") {
    return { label: "Backend unavailable", tone: "warning" };
  }
  return { label: "Checking backend", tone: "neutral" };
}

function describeLeadLoadState(state: LeadTableState, error: string, hasRows: boolean): string {
  if (state === "retrying") {
    return hasRows ? "Database busy or backend reconnecting. Showing the last successful worksheet while retrying." : "Database busy, retrying worksheet load.";
  }
  if (state === "loading") {
    return "Lead data still loading.";
  }
  if (state === "error") {
    return error || "Backend unavailable.";
  }
  if (state === "empty") {
    return "No leads match the current filters.";
  }
  return "";
}

function humanizeReason(reason: string) {
  const [bucket, ...rest] = reason.split(":");
  if (!rest.length) {
    return humanizeToken(reason);
  }
  return `${humanizeToken(bucket)}: ${rest.join(":").trim().replaceAll("_", " ")}`;
}

function humanizeSharedSignal(signal: string) {
  const [key, rawValue] = signal.split(":");
  if (key === "shared_high_signal_tech" && rawValue) {
    return `Shared tech: ${rawValue
      .split(",")
      .map((item) => humanizeToken(item.trim()))
      .join(", ")}`;
  }
  return humanizeToken(signal);
}

function colourForToken(value: string) {
  if (PLATFORM_COLOURS[value]) {
    return PLATFORM_COLOURS[value];
  }
  let hash = 0;
  for (let index = 0; index < value.length; index += 1) {
    hash = (hash * 31 + value.charCodeAt(index)) % COLOUR_FALLBACKS.length;
  }
  return COLOUR_FALLBACKS[Math.abs(hash) % COLOUR_FALLBACKS.length];
}

function confidenceTone(value: string | undefined) {
  const normalized = (value || "").toLowerCase();
  if (normalized === "high" || normalized === "strong" || normalized === "confirmed" || normalized === "success" || normalized === "up" || normalized === "ok") {
    return "positive";
  }
  if (normalized === "medium" || normalized === "moderate" || normalized === "possible" || normalized === "probable" || normalized === "redirect" || normalized === "blocked") {
    return "warning";
  }
  if (
    normalized === "low" ||
    normalized === "weak" ||
    normalized === "none" ||
    normalized === "partial" ||
    normalized === "overlap" ||
    normalized === "historic" ||
    normalized === "network"
  ) {
    return "neutral";
  }
  if (normalized === "error" || normalized === "down" || normalized === "not_found" || normalized === "server_error" || normalized === "client_error" || normalized === "timeout" || normalized === "dns_error" || normalized === "ssl_error" || normalized === "other_error") {
    return "warning";
  }
  return "neutral";
}

function pillList(values: string[], limit = 3) {
  if (!values.length) {
    return <span className="muted">None</span>;
  }
  return values.slice(0, limit).map((value) => (
    <span className={`pill ${pillToneClass(value)}`} key={value}>
      {humanizeToken(value)}
    </span>
  ));
}

function pillToneClass(value: string): string {
  const normalized = value.trim().toLowerCase();
  if (
    [
      "strong collection intro",
      "customised titles",
      "meaningful collection depth",
    ].includes(normalized)
  ) {
    return "pill-positive";
  }
  if (
    [
      "default_collection_title",
      "collection_content_gap",
      "collection_content_unclear",
      "collection_title_needs_review",
      "templated_titles",
      "no_collection_copy",
      "missing_intro",
      "boilerplate_only",
      "mixed_low_confidence",
      "default_exact",
      "default_like",
      "recrawl_slower",
      "crawl_failed",
      "internal_errors",
      "schema_gap",
    ].includes(normalized)
  ) {
    return "pill-warning";
  }
  return "pill-neutral";
}

function screamingFrogStrengths(lead: Lead) {
  const strengths: string[] = [];
  if (!lead.screamingfrog_status) {
    return strengths;
  }
  if ((lead.screamingfrog_result_reason || "") === "rate_limited_429" || (lead.screamingfrog_status || "") === "error") {
    return strengths;
  }
  if ((lead.screamingfrog_collection_issue_family || "") === "collection_page_not_reviewable") {
    return strengths;
  }
  if ((lead.screamingfrog_collection_intro_status || "") === "strong_intro" && Number(lead.screamingfrog_collection_best_intro_confidence || 0) >= 75) {
    strengths.push("strong collection intro");
  }
  if (
    (lead.screamingfrog_title_optimization_status || "") === "customised" &&
    Number(lead.screamingfrog_title_optimization_confidence || lead.screamingfrog_collection_title_rule_confidence || 0) >= 70
  ) {
    strengths.push("customised titles");
  }
  if (
    Number(lead.screamingfrog_collection_product_count || 0) >= 8 &&
    Number(lead.screamingfrog_collection_detection_confidence || 0) >= 75
  ) {
    strengths.push("meaningful collection depth");
  }
  return strengths;
}

function screamingFrogIssueSignals(lead: Lead) {
  const issues: string[] = [];
  if (!lead.screamingfrog_status) {
    return issues;
  }
  const pushUnique = (value: string | null | undefined) => {
    const normalized = (value || "").trim();
    if (!normalized || issues.includes(normalized)) {
      return;
    }
    issues.push(normalized);
  };
  if ((lead.screamingfrog_result_reason || "") === "rate_limited_429") {
    pushUnique("recrawl_slower");
  }
  const titleStatus = (lead.screamingfrog_title_optimization_status || "").trim();
  const titleConfidence = Number(lead.screamingfrog_title_optimization_confidence || lead.screamingfrog_collection_title_rule_confidence || 0);
  if (titleStatus === "default_exact") {
    pushUnique("templated_titles");
  } else if (titleStatus === "default_like" && titleConfidence >= 80) {
    pushUnique("templated_titles");
  }
  const introStatus = (lead.screamingfrog_collection_intro_status || "").trim();
  const introConfidence = Number(lead.screamingfrog_collection_best_intro_confidence || lead.screamingfrog_collection_intro_confidence || 0);
  if (
    ["missing_intro", "boilerplate_only"].includes(introStatus) ||
    (introStatus === "mixed_low_confidence" && introConfidence < 55)
  ) {
    pushUnique("no_collection_copy");
  }
  pushUnique(lead.screamingfrog_collection_issue_family);
  pushUnique(lead.screamingfrog_primary_issue_family);
  lead.screamingfrog_collection_content_issue_flags.forEach(pushUnique);
  lead.screamingfrog_default_title_issue_flags.forEach(pushUnique);
  lead.screamingfrog_schema_issue_flags.forEach(pushUnique);
  lead.screamingfrog_product_metadata_issue_flags.forEach(pushUnique);
  lead.screamingfrog_homepage_issue_flags.forEach(pushUnique);
  if (Number(lead.screamingfrog_has_internal_errors || 0)) {
    pushUnique("internal_errors");
  }
  const priority = new Map<string, number>([
    ["recrawl_slower", 0],
    ["crawl_failed", 1],
    ["templated_titles", 2],
    ["default_collection_title", 3],
    ["no_collection_copy", 4],
    ["collection_content_gap", 5],
    ["collection_content_unclear", 6],
    ["collection_title_needs_review", 7],
    ["schema_gap", 8],
    ["internal_errors", 9],
  ]);
  return issues
    .filter(Boolean)
    .sort((left, right) => {
      const leftPriority = priority.get(left) ?? 99;
      const rightPriority = priority.get(right) ?? 99;
      if (leftPriority !== rightPriority) {
        return leftPriority - rightPriority;
      }
      return left.localeCompare(right);
    });
}

function screamingFrogHeadingHealth(lead: Lead) {
  const issues = lead.screamingfrog_heading_issue_flags ?? [];
  if (!lead.screamingfrog_status) {
    return { label: "Not audited", note: "No heading read", tone: "neutral" as const };
  }
  if (!issues.length) {
    return { label: "Healthy", note: lead.screamingfrog_heading_outline_summary || "No major heading issues", tone: "positive" as const };
  }
  return {
    label: humanizeToken(issues[0]),
    note: lead.screamingfrog_heading_outline_summary || `${issues.length} heading flags`,
    tone: "warning" as const,
  };
}

function sourceCoverageLabel(value: string): string {
  switch (value) {
    case "complete":
      return "Complete timing";
    case "partial":
      return "Partial timing";
    case "partial_recent_only":
      return "Current + recent";
    case "current_only":
      return "Current only";
    default:
      return "Untrusted";
  }
}

function sourceCoverageTone(value: string): "positive" | "warning" | "neutral" {
  if (value === "complete") {
    return "positive";
  }
  if (value === "partial" || value === "partial_recent_only" || value === "current_only") {
    return "warning";
  }
  return "neutral";
}

function screamingFrogCollectionTitleSignal(lead: Lead) {
  if (!lead.screamingfrog_status) {
    return { label: "Not audited", note: "No collection title read", tone: "neutral" as const };
  }
  if ((lead.screamingfrog_collection_issue_family || "") === "collection_page_not_reviewable") {
    return { label: "Unavailable", note: "No reviewable collection/category page captured", tone: "neutral" as const };
  }
  if ((lead.screamingfrog_result_reason || "") === "rate_limited_429") {
    return { label: "Recrawl slower", note: "429 rate limited", tone: "warning" as const };
  }
  const status = (lead.screamingfrog_title_optimization_status || "").trim();
  const confidence = Number(lead.screamingfrog_title_optimization_confidence || lead.screamingfrog_collection_title_rule_confidence || 0);
  switch (status) {
    case "default_exact":
      return { label: "Default CMS title", note: lead.screamingfrog_collection_title_value || "Matches a default collection title pattern", tone: "warning" as const };
    case "default_like":
      return confidence >= 80
        ? { label: "Default-like title", note: lead.screamingfrog_collection_title_value || "Looks close to a CMS template title", tone: "warning" as const }
        : { label: "Needs review", note: lead.screamingfrog_collection_title_value || "Looks templated but confidence is low", tone: "neutral" as const };
    case "term_plus_site":
      return { label: "Templated", note: lead.screamingfrog_collection_title_value || "Looks lightly templated", tone: "neutral" as const };
    case "customised":
      return { label: "Customised", note: lead.screamingfrog_collection_title_value || "Title looks intentionally written", tone: "positive" as const };
    default:
      return { label: "Unknown", note: lead.screamingfrog_collection_title_value || "No strong collection title read", tone: "neutral" as const };
  }
}

function screamingFrogCollectionSnippet(lead: Lead) {
  if ((lead.screamingfrog_collection_issue_family || "") === "collection_page_not_reviewable") {
    return { label: "No reliable page", note: "The crawl did not capture a reviewable collection/category page", tone: "warning" as const };
  }
  const snippet =
    (lead.screamingfrog_collection_best_intro_text || "").trim() ||
    (lead.screamingfrog_collection_above_clean_text || "").trim() ||
    (lead.screamingfrog_collection_below_clean_text || "").trim();
  if (!snippet) {
    return { label: "No cleaned snippet", note: "No trustworthy collection copy extracted", tone: "neutral" as const };
  }
  const compact = snippet.length > 160 ? `${snippet.slice(0, 157).trim()}...` : snippet;
  const position = humanizeToken(
    (lead.screamingfrog_collection_best_intro_position || lead.screamingfrog_collection_intro_position || "unknown").replace("near_grid", "near grid"),
  );
  return { label: position, note: compact, tone: "neutral" as const };
}

function screamingFrogCollectionConfidence(lead: Lead) {
  if (!lead.screamingfrog_status) {
    return { label: "Not audited", note: "No collection read", tone: "neutral" as const };
  }
  if ((lead.screamingfrog_collection_issue_family || "") === "collection_page_not_reviewable") {
    return { label: "N/A", note: "No reviewable collection/category page captured", tone: "warning" as const };
  }
  const confidence = Number(lead.screamingfrog_collection_best_intro_confidence || lead.screamingfrog_collection_intro_confidence || 0);
  if (confidence >= 80) {
    return { label: `${confidence}%`, note: "High-confidence intro read", tone: "positive" as const };
  }
  if (confidence >= 55) {
    return { label: `${confidence}%`, note: "Usable but mixed", tone: "neutral" as const };
  }
  return { label: `${confidence}%`, note: "Low-confidence extraction", tone: "warning" as const };
}

function screamingFrogEvidenceGrade(lead: Lead) {
  if (!lead.screamingfrog_status) {
    return { label: "Not audited", note: "No crawl evidence", tone: "neutral" as const };
  }
  if ((lead.screamingfrog_result_reason || "") === "rate_limited_429") {
    return { label: "Recrawl slower", note: "429 rate limited", tone: "warning" as const };
  }
  const pages = Number(lead.screamingfrog_pages_crawled || 0);
  const seeds = Number(lead.screamingfrog_seed_count || 0);
  const categories = Number(lead.screamingfrog_category_page_count || 0);
  const products = Number(lead.screamingfrog_product_page_count || 0);
  const confidence = Number(lead.screamingfrog_collection_detection_confidence || 0);
  const resultReason = (lead.screamingfrog_result_reason || "").trim();

  if ((lead.screamingfrog_status || "") === "error") {
    return { label: "F", note: "Crawl failed", tone: "warning" as const };
  }
  if (["no_useful_seeds_found", "redirect_only_homepage", "homepage_fetch_failed"].includes(resultReason) || pages <= 1 || seeds <= 1) {
    return { label: "D", note: "Thin crawl evidence", tone: "warning" as const };
  }
  if (categories >= 5 && pages >= 7 && confidence >= 80) {
    return { label: "A", note: `${categories} category pages captured`, tone: "positive" as const };
  }
  if (categories >= 3 && pages >= 5 && confidence >= 70) {
    return { label: "B", note: `${categories} category pages captured`, tone: "positive" as const };
  }
  if (pages >= 3 && (categories >= 1 || products >= 1 || lead.screamingfrog_seed_strategy === "sitemap")) {
    return { label: "C", note: "Usable but limited coverage", tone: "neutral" as const };
  }
  return { label: "D", note: "Weak crawl evidence", tone: "warning" as const };
}

function matchingReasonText(lead: Lead, selectedBuckets: string[]) {
  const reasons = lead.bucket_reasons_list;
  if (!selectedBuckets.length) {
    return reasons.slice(0, 2).map(humanizeReason).join(" · ");
  }
  const matching = reasons.filter((reason) => selectedBuckets.some((bucket) => reason.startsWith(`${bucket}:`)));
  return (matching.length ? matching : reasons)
    .slice(0, 3)
    .map(humanizeReason)
    .join(" · ");
}

function formatNumber(value: number | string | undefined) {
  const numeric = Number(value || 0);
  if (!numeric) {
    return "—";
  }
  return numeric.toLocaleString();
}

function formatSignedNumber(value: number | string | undefined) {
  const numeric = Number(value);
  if (Number.isNaN(numeric) || numeric === 0) {
    return numeric === 0 ? "0" : "—";
  }
  return `${numeric > 0 ? "+" : ""}${numeric.toLocaleString()}`;
}

function formatPercent(value: number | string | undefined) {
  const numeric = Number(value);
  if (Number.isNaN(numeric)) {
    return "—";
  }
  return `${numeric > 0 ? "+" : ""}${numeric.toFixed(1)}%`;
}

function formatDate(value: string | undefined, fallback = "—") {
  if (!value) {
    return fallback;
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return parsed.toLocaleDateString();
}

function sortBadge(query: LeadQuery) {
  return `${sortLabels[query.sortBy] ?? query.sortBy} · ${query.sortDirection.toUpperCase()}`;
}

function columnSortKey(column: ColumnKey) {
  switch (column) {
    case "country":
      return "country";
    case "vertical":
      return "vertical";
    case "domain_migration":
      return "domain_migration_status";
    case "cms_migration":
      return "cms_migration_status";
    case "domain_fingerprint_strength":
      return "domain_fingerprint_strength";
    case "technology_spend":
      return "technology_spend";
    case "total_score":
      return "total_score";
    case "priority_tier":
      return "priority_tier";
    case "sales_buckets":
      return "bucket_count";
    case "contact_status":
      return "contact_score";
    case "matched_first_detected":
      return "matched_first_detected";
    case "matched_last_found":
      return "matched_last_found";
    case "cms_migration_date":
      return "cms_migration_likely_date";
    case "domain_migration_date":
      return "domain_migration_estimated_date";
    case "se_traffic_change":
      return "se_ranking_traffic_delta_percent";
    case "se_keyword_change":
      return "se_ranking_keywords_delta_percent";
    case "se_checked":
      return "se_ranking_checked_at";
    case "site_status_code":
      return "site_status_code";
    case "site_checked":
      return "site_status_checked_at";
    case "sf_checked":
      return "screamingfrog_checked_at";
    case "sf_pages_crawled":
      return "screamingfrog_pages_crawled";
    case "sf_score":
      return "screamingfrog_opportunity_score";
    case "sf_collection_detection":
      return "screamingfrog_collection_detection_confidence";
    case "sf_collection_products":
      return "screamingfrog_collection_product_count";
    default:
      return "";
  }
}

function isDefaultTimelineEventSelection(values: TimelineEventType[]) {
  return (
    values.length === DEFAULT_TIMELINE_EVENT_TYPES.length &&
    DEFAULT_TIMELINE_EVENT_TYPES.every((value) => values.includes(value))
  );
}

function formatMonthYear(value: string | undefined) {
  if (!value) {
    return "—";
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return parsed.toLocaleDateString(undefined, { month: "short", year: "numeric" });
}

function dateWindowLabel(from: string, to: string) {
  if (from && to) {
    return `${from} → ${to}`;
  }
  if (from) {
    return `From ${from}`;
  }
  if (to) {
    return `To ${to}`;
  }
  return "All time";
}

function isoDate(value: Date) {
  return value.toISOString().slice(0, 10);
}

function screamingFrogAuditViewerUrl(rootDomain: string) {
  return `${BACKEND_APP_BASE}/api/screamingfrog/audit/open?root_domain=${encodeURIComponent(rootDomain)}`;
}

function startOfMonth(value: Date) {
  return new Date(Date.UTC(value.getUTCFullYear(), value.getUTCMonth(), 1));
}

function shiftUtcMonths(value: Date, delta: number) {
  return new Date(Date.UTC(value.getUTCFullYear(), value.getUTCMonth() + delta, 1));
}

function migrationPresetWindow(months: number) {
  const today = new Date();
  return {
    from: isoDate(startOfMonth(shiftUtcMonths(today, -(months - 1)))),
    to: isoDate(today),
  };
}

function midpointDate(first: string | undefined, second: string | undefined) {
  if (!first || !second) {
    return null;
  }
  const firstTs = Date.parse(first);
  const secondTs = Date.parse(second);
  if (Number.isNaN(firstTs) || Number.isNaN(secondTs)) {
    return null;
  }
  return new Date((firstTs + secondTs) / 2).toISOString().slice(0, 10);
}

function seDeltaToneClass(value: number | string | undefined) {
  const numeric = Number(value ?? 0);
  if (!Number.isFinite(numeric) || numeric === 0) {
    return "se-delta-neutral";
  }
  const abs = Math.abs(numeric);
  if (numeric > 0) {
    if (abs >= 50) return "se-delta-positive-strong";
    if (abs >= 20) return "se-delta-positive-medium";
    return "se-delta-positive-soft";
  }
  if (abs >= 50) return "se-delta-negative-strong";
  if (abs >= 20) return "se-delta-negative-medium";
  return "se-delta-negative-soft";
}

function inferredExactDomain(search: string) {
  const trimmed = search.trim().toLowerCase();
  if (!trimmed || trimmed.includes(" ")) {
    return "";
  }
  if (!trimmed.includes(".")) {
    return "";
  }
  return trimmed.replace(/^https?:\/\//, "").replace(/^www\./, "").replace(/\/$/, "");
}

function evidenceQualityLabel(lead: Lead) {
  if (lead.domain_migration_status === "confirmed" || lead.cms_migration_status === "confirmed") {
    return "Confirmed answer";
  }
  if (
    ["probable", "network", "weak"].includes(lead.domain_migration_status) ||
    ["possible", "historic", "overlap", "removed_only"].includes(lead.cms_migration_status)
  ) {
    return "Possible interpretation";
  }
  return "Discovery view";
}

export default function App() {
  const [summary, setSummary] = useState<SummaryResponse | null>(null);
  const [options, setOptions] = useState<FilterOptions | null>(null);
  const [presets, setPresets] = useState<Preset[]>([]);
  const [query, setQuery] = useState<LeadQuery>(initialQuery);
  const [visibleColumns, setVisibleColumns] = useState<ColumnKey[]>(defaultVisibleColumns);
  const [leads, setLeads] = useState<LeadsResponse | null>(null);
  const [tray, setTray] = useState<ExportTrayResponse | null>(null);
  const [selectedLeadId, setSelectedLeadId] = useState<string | null>(null);
  const [detail, setDetail] = useState<LeadDetailResponse | null>(null);
  const [currentPresetId, setCurrentPresetId] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [detailLoading, setDetailLoading] = useState(false);
  const [selectionLoading, setSelectionLoading] = useState(false);
  const [error, setError] = useState("");
  const [backendHealth, setBackendHealth] = useState<HealthResponse | null>(null);
  const [backendState, setBackendState] = useState<BackendConnectionState>("checking");
  const [leadTableState, setLeadTableState] = useState<LeadTableState>("loading");
  const [toast, setToast] = useState("");
  const [searchDraft, setSearchDraft] = useState(initialQuery.search);
  const [exactDomainDraft, setExactDomainDraft] = useState(initialQuery.exactDomain);
  const [verticalSearch, setVerticalSearch] = useState("");
  const [drawerPending, setDrawerPending] = useState<"first" | "last" | null>(null);
  const [showColumnChooser, setShowColumnChooser] = useState(false);
  const [presetNameDraft, setPresetNameDraft] = useState("");
  const [presetModal, setPresetModal] = useState<null | { mode: "save" | "delete" }>(null);
  const [showGuideModal, setShowGuideModal] = useState(false);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [sidebarSections, setSidebarSections] = useState({
    search: false,
    common: false,
    migration: false,
    analysis: false,
    advanced: false,
  });
  const [trayCollapsed, setTrayCollapsed] = useState(true);
  const [spreadsheetFocusMode, setSpreadsheetFocusMode] = useState(false);
  const [browserFullscreen, setBrowserFullscreen] = useState(false);
  const [seRankingType, setSeRankingType] = useState<SeRankingAnalysisType>("cms_migration");
  const [seRankingSummary, setSeRankingSummary] = useState<SeRankingSummaryResponse | null>(null);
  const [seRankingSummaryDirty, setSeRankingSummaryDirty] = useState(true);
  const [seRankingLoading, setSeRankingLoading] = useState(false);
  const [manualSeFirstMonth, setManualSeFirstMonth] = useState("");
  const [manualSeSecondMonth, setManualSeSecondMonth] = useState("");
  const [manualSePreview, setManualSePreview] = useState<SeRankingManualPreviewResponse | null>(null);
  const [manualSeLoading, setManualSeLoading] = useState(false);
  const [siteStatusSummary, setSiteStatusSummary] = useState<SiteStatusSummaryResponse | null>(null);
  const [siteStatusLoading, setSiteStatusLoading] = useState(false);
  const [screamingFrogCrawlMode, setScreamingFrogCrawlMode] = useState<ScreamingFrogCrawlMode>("bounded_audit");
  const [screamingFrogSummary, setScreamingFrogSummary] = useState<ScreamingFrogSummaryResponse | null>(null);
  const [screamingFrogLoading, setScreamingFrogLoading] = useState(false);
  const [screamingFrogRunStatus, setScreamingFrogRunStatus] = useState<InlineRunStatus>(null);
  const [screamingFrogRecentResults, setScreamingFrogRecentResults] = useState<ScreamingFrogRunResponse["results"]>([]);
  const [screamingFrogJobBatch, setScreamingFrogJobBatch] = useState<ScreamingFrogJobBatch | null>(null);

  const deferredSearch = useDeferredValue(query.search);
  const deferredVerticalSearch = useDeferredValue(verticalSearch);
  const debouncedQuery = useDebouncedValue(query, 180);
  const filteredVerticalOptions = (options?.verticals ?? []).filter((vertical) =>
    vertical.toLowerCase().includes(deferredVerticalSearch.trim().toLowerCase()),
  );
  const shouldShowVerticalSearch = (options?.verticals?.length ?? 0) > FILTER_SEARCH_THRESHOLD;
  const traySet = useMemo(() => new Set(tray?.rootDomains ?? []), [tray]);
  const traySignature = useMemo(() => (tray?.rootDomains ?? []).join("|"), [tray]);
  const activePreset = presets.find((preset) => preset.id === currentPresetId) ?? null;
  const selectedLead = leads?.items.find((item) => item.root_domain === selectedLeadId) ?? null;
  const backendPill = describeBackendState(backendState, backendHealth);
  const leadStateMessage = describeLeadLoadState(leadTableState, error, Boolean(leads?.items.length));
  const hasTimelineSelection = query.timelinePlatforms.length > 0;
  const hasVisibleSeRankingData = Boolean(
    leads?.items.some(
      (item) =>
        Boolean(item.se_ranking_status) ||
        Boolean(item.se_ranking_checked_at) ||
        Boolean(item.se_ranking_market),
    ),
  );
  const hasVisibleSiteStatusData = Boolean(
    leads?.items.some((item) => Boolean(item.site_status_category) || Boolean(item.site_status_checked_at)),
  );
  const hasVisibleScreamingFrogData = Boolean(
    leads?.items.some((item) => Boolean(item.screamingfrog_status) || Boolean(item.screamingfrog_checked_at)),
  );
  const hasCmsTiming = Boolean(query.cmsMigrationFrom || query.cmsMigrationTo);
  const hasDomainTiming = Boolean(query.domainMigrationFrom || query.domainMigrationTo);
  const hasManualSeMonths = Boolean(manualSeFirstMonth && manualSeSecondMonth && manualSeFirstMonth !== manualSeSecondMonth);
  const migrationTimingLogicLabel = query.migrationTimingOperator === "or" ? "Match either window" : "Match both windows";
  const effectiveSidebarCollapsed = spreadsheetFocusMode || sidebarCollapsed;
  const effectiveTrayCollapsed = spreadsheetFocusMode || trayCollapsed;
  const appShellClassName = [
    "app-shell",
    spreadsheetFocusMode ? "spreadsheet-focus-mode" : "",
    effectiveSidebarCollapsed ? "sidebar-collapsed" : "",
    effectiveTrayCollapsed ? "tray-collapsed" : "",
    browserFullscreen ? "browser-fullscreen" : "",
  ]
    .filter(Boolean)
    .join(" ");
  const effectiveVisibleColumns = useMemo<ColumnKey[]>(() => {
    let normalizedVisibleColumns = ensureColumns(visibleColumns);
    if (hasTimelineSelection) {
      const requiredTimeline: ColumnKey[] = ["matched_timeline_platforms", "matched_first_detected", "matched_last_found"];
      normalizedVisibleColumns = [
        ...normalizedVisibleColumns,
        ...requiredTimeline.filter((column) => !normalizedVisibleColumns.includes(column)),
      ];
    }
    if (hasVisibleSeRankingData) {
      const requiredSeRanking: ColumnKey[] = ["se_market", "se_traffic_change", "se_keyword_change", "se_outcome", "se_checked"];
      normalizedVisibleColumns = [
        ...normalizedVisibleColumns,
        ...requiredSeRanking.filter((column) => !normalizedVisibleColumns.includes(column)),
      ];
    }
    if (hasVisibleSiteStatusData) {
      const requiredSiteStatus: ColumnKey[] = ["site_status", "site_status_code", "site_checked"];
      normalizedVisibleColumns = [
        ...normalizedVisibleColumns,
        ...requiredSiteStatus.filter((column) => !normalizedVisibleColumns.includes(column)),
      ];
    }
    if (hasVisibleScreamingFrogData) {
      const requiredScreamingFrog: ColumnKey[] = [
        "sf_status",
        "sf_config",
        "sf_quality",
        "sf_score",
        "sf_primary_issue",
        "sf_issue_signals",
        "sf_strengths",
        "sf_evidence",
        "sf_heading_health",
        "sf_collection_title_signal",
        "sf_collection_intro",
        "sf_collection_snippet",
        "sf_homepage_status",
        "sf_internal_errors",
        "sf_checked",
      ];
      normalizedVisibleColumns = [
        ...normalizedVisibleColumns,
        ...requiredScreamingFrog.filter((column) => !normalizedVisibleColumns.includes(column)),
      ];
    }
    return normalizedVisibleColumns;
  }, [hasTimelineSelection, hasVisibleSeRankingData, hasVisibleSiteStatusData, hasVisibleScreamingFrogData, visibleColumns]);
  const groupedPresets = useMemo(() => {
    const groups = new Map<string, Preset[]>();
    presets.forEach((preset) => {
      const list = groups.get(preset.group) ?? [];
      list.push(preset);
      groups.set(preset.group, list);
    });
    return Array.from(groups.entries());
  }, [presets]);
  const leadsRequestQuery = useMemo(
    () => normalizeLeadQuery({ ...debouncedQuery, search: deferredSearch }),
    [debouncedQuery, deferredSearch],
  );
  const scopedRequestQuery = useMemo(
    () => normalizeLeadQuery({ ...debouncedQuery, search: deferredSearch, page: 1 }),
    [debouncedQuery, deferredSearch],
  );
  useEffect(() => {
    if (!toast) {
      return undefined;
    }
    const timeout = window.setTimeout(() => setToast(""), 2400);
    return () => window.clearTimeout(timeout);
  }, [toast]);

  useEffect(() => {
    setSearchDraft(query.search);
  }, [query.search]);

  useEffect(() => {
    setExactDomainDraft(query.exactDomain);
  }, [query.exactDomain]);

  useEffect(() => {
    const normalizedDraft = exactDomainDraft.trim().toLowerCase();
    if (normalizedDraft === query.exactDomain) {
      return undefined;
    }
    const timeout = window.setTimeout(() => {
      updateQuery({ exactDomain: normalizedDraft });
    }, 180);
    return () => window.clearTimeout(timeout);
  }, [exactDomainDraft, query.exactDomain]);

  useEffect(() => {
    const syncFullscreen = () => {
      setBrowserFullscreen(Boolean(document.fullscreenElement));
    };
    syncFullscreen();
    document.addEventListener("fullscreenchange", syncFullscreen);
    return () => document.removeEventListener("fullscreenchange", syncFullscreen);
  }, []);

  useEffect(() => {
    document.title = "Lead Console";
  }, []);

  useEffect(() => {
    setSeRankingSummaryDirty(true);
    setSeRankingSummary(null);
  }, [seRankingType, query]);

  useEffect(() => {
    let cancelled = false;
    async function loadSiteStatusSummary() {
      if (!tray?.count) {
        setSiteStatusSummary(null);
        setSiteStatusLoading(false);
        return;
      }
      setSiteStatusLoading(true);
      try {
        const response = await fetchSiteStatusSummary();
        if (!cancelled) {
          setSiteStatusSummary(response);
        }
      } catch (loadError) {
        if (!cancelled) {
          setError(loadError instanceof Error ? loadError.message : "Failed to load site status summary");
        }
      } finally {
        if (!cancelled) {
          setSiteStatusLoading(false);
        }
      }
    }
    void loadSiteStatusSummary();
    return () => {
      cancelled = true;
    };
  }, [traySignature]);

  useEffect(() => {
    let cancelled = false;
    async function loadScreamingFrogSummary() {
      if (!tray?.count) {
        setScreamingFrogSummary(null);
        setScreamingFrogJobBatch(null);
        setScreamingFrogLoading(false);
        return;
      }
      setScreamingFrogLoading(true);
      try {
        const response = await fetchScreamingFrogSummary(screamingFrogCrawlMode);
        if (!cancelled) {
          setScreamingFrogSummary(response);
          setScreamingFrogJobBatch(response.jobBatch ?? null);
        }
      } catch (loadError) {
        if (!cancelled) {
          setError(loadError instanceof Error ? loadError.message : "Failed to load Screaming Frog summary");
        }
      } finally {
        if (!cancelled) {
          setScreamingFrogLoading(false);
        }
      }
    }
    void loadScreamingFrogSummary();
    return () => {
      cancelled = true;
    };
  }, [screamingFrogCrawlMode, traySignature]);

  useEffect(() => {
    if (!screamingFrogJobBatch?.batchId || !screamingFrogJobBatch.isActive) {
      return undefined;
    }
    let cancelled = false;
    const interval = window.setInterval(async () => {
      try {
        const batch = await fetchScreamingFrogJobStatus(screamingFrogJobBatch.batchId);
        if (cancelled) {
          return;
        }
        setScreamingFrogJobBatch(batch);
        if (!batch.isActive) {
          const completedErrors = (batch.counts.error ?? 0) + (batch.counts.partial ?? 0);
          setScreamingFrogRunStatus({
            tone: completedErrors ? "warning" : "positive",
            label: completedErrors ? "Audit finished with issues" : "Audit complete",
            message: `Success ${batch.counts.success ?? 0} · Partial ${batch.counts.partial ?? 0} · Error ${batch.counts.error ?? 0}`,
          });
          await reloadWorksheet();
          const refreshed = await fetchScreamingFrogSummary(screamingFrogCrawlMode);
          if (!cancelled) {
            setScreamingFrogSummary(refreshed);
            setScreamingFrogJobBatch(refreshed.jobBatch ?? batch);
          }
        }
      } catch (loadError) {
        if (!cancelled) {
          setError(loadError instanceof Error ? loadError.message : "Failed to load Screaming Frog job status");
        }
      }
    }, 1500);
    return () => {
      cancelled = true;
      window.clearInterval(interval);
    };
  }, [screamingFrogJobBatch?.batchId, screamingFrogJobBatch?.isActive, screamingFrogCrawlMode]);

  useEffect(() => {
    setManualSePreview(null);
  }, [manualSeFirstMonth, manualSeSecondMonth, traySignature]);

  const updateQuery = useCallback((patch: Partial<LeadQuery>, keepPreset = false) => {
    startTransition(() => {
      setQuery((current) => {
        const nextSearch = patch.search ?? current.search;
        const exactDomain = patch.exactDomain ?? inferredExactDomain(nextSearch);
        return normalizeLeadQuery({ ...current, ...patch, exactDomain, page: patch.page ?? 1 });
      });
      if (!keepPreset) {
        setCurrentPresetId(null);
      }
    });
  }, []);

  const activeFilterChips = useMemo(() => [
    ...(query.search ? [{ group: "Search", label: query.search, clear: () => updateQuery({ search: "" }) }] : []),
    ...(query.exactDomain
      ? [{ group: "Exact domain", label: query.exactDomain, clear: () => updateQuery({ exactDomain: "", search: "" }) }]
      : []),
    ...query.currentPlatforms.map((value) => ({
      group: "Current",
      label: humanizeToken(value),
      clear: () => updateQuery({ currentPlatforms: query.currentPlatforms.filter((item) => item !== value) }),
    })),
    ...query.recentPlatforms.map((value) => ({
      group: "Recent",
      label: humanizeToken(value),
      clear: () => updateQuery({ recentPlatforms: query.recentPlatforms.filter((item) => item !== value) }),
    })),
    ...query.removedPlatforms.map((value) => ({
      group: "Removed",
      label: humanizeToken(value),
      clear: () => updateQuery({ removedPlatforms: query.removedPlatforms.filter((item) => item !== value) }),
    })),
    ...query.domainConfidenceBands.map((value) => ({
      group: "Domain confidence",
      label: humanizeToken(value),
      clear: () => updateQuery({ domainConfidenceBands: query.domainConfidenceBands.filter((item) => item !== value) }),
    })),
    ...query.domainMigrationStatuses.map((value) => ({
      group: "Previous domain",
      label: humanizeToken(value),
      clear: () => updateQuery({ domainMigrationStatuses: query.domainMigrationStatuses.filter((item) => item !== value) }),
    })),
    ...query.domainFingerprintStrengths.map((value) => ({
      group: "Fingerprint",
      label: humanizeToken(value),
      clear: () =>
        updateQuery({ domainFingerprintStrengths: query.domainFingerprintStrengths.filter((item) => item !== value) }),
    })),
    ...query.domainTldRelationships.map((value) => ({
      group: "TLD",
      label: humanizeToken(value),
      clear: () => updateQuery({ domainTldRelationships: query.domainTldRelationships.filter((item) => item !== value) }),
    })),
    ...query.cmsConfidenceLevels.map((value) => ({
      group: "CMS confidence",
      label: humanizeToken(value),
      clear: () => updateQuery({ cmsConfidenceLevels: query.cmsConfidenceLevels.filter((item) => item !== value) }),
    })),
    ...query.cmsMigrationStatuses.map((value) => ({
      group: "CMS status",
      label: humanizeToken(value),
      clear: () => updateQuery({ cmsMigrationStatuses: query.cmsMigrationStatuses.filter((item) => item !== value) }),
    })),
    ...query.marketingPlatforms.map((value) => ({
      group: "Marketing",
      label: humanizeToken(value),
      clear: () => updateQuery({ marketingPlatforms: query.marketingPlatforms.filter((item) => item !== value) }),
    })),
    ...query.crmPlatforms.map((value) => ({
      group: "CRM",
      label: humanizeToken(value),
      clear: () => updateQuery({ crmPlatforms: query.crmPlatforms.filter((item) => item !== value) }),
    })),
    ...query.paymentPlatforms.map((value) => ({
      group: "Payments",
      label: humanizeToken(value),
      clear: () => updateQuery({ paymentPlatforms: query.paymentPlatforms.filter((item) => item !== value) }),
    })),
    ...query.hostingProviders.map((value) => ({
      group: "Hosting",
      label: humanizeToken(value),
      clear: () => updateQuery({ hostingProviders: query.hostingProviders.filter((item) => item !== value) }),
    })),
    ...query.agencies.map((value) => ({
      group: "Agency",
      label: humanizeToken(value),
      clear: () => updateQuery({ agencies: query.agencies.filter((item) => item !== value) }),
    })),
    ...query.aiTools.map((value) => ({
      group: "AI",
      label: humanizeToken(value),
      clear: () => updateQuery({ aiTools: query.aiTools.filter((item) => item !== value) }),
    })),
    ...query.complianceFlags.map((value) => ({
      group: "Compliance",
      label: humanizeToken(value),
      clear: () => updateQuery({ complianceFlags: query.complianceFlags.filter((item) => item !== value) }),
    })),
    ...(query.minSocial ? [{ group: "Followers", label: `Min ${formatNumber(query.minSocial)}`, clear: () => updateQuery({ minSocial: "" }) }] : []),
    ...(query.minRevenue ? [{ group: "Revenue", label: `Min ${formatNumber(query.minRevenue)}`, clear: () => updateQuery({ minRevenue: "" }) }] : []),
    ...(query.minEmployees ? [{ group: "Employees", label: `Min ${formatNumber(query.minEmployees)}`, clear: () => updateQuery({ minEmployees: "" }) }] : []),
    ...(query.minSku ? [{ group: "SKU", label: `Min ${formatNumber(query.minSku)}`, clear: () => updateQuery({ minSku: "" }) }] : []),
    ...(query.minTechnologySpend
      ? [{ group: "Tech spend", label: `Min ${formatNumber(query.minTechnologySpend)}`, clear: () => updateQuery({ minTechnologySpend: "" }) }]
      : []),
    ...(query.hasSeRankingAnalysis
      ? [{ group: "SE Ranking", label: "Analyzed only", clear: () => updateQuery({ hasSeRankingAnalysis: false }) }]
      : []),
    ...query.seRankingAnalysisTypes.map((value) => ({
      group: "SE Ranking",
      label: humanizeToken(value),
      clear: () => updateQuery({ seRankingAnalysisTypes: query.seRankingAnalysisTypes.filter((item) => item !== value) }),
    })),
    ...query.seRankingOutcomeFlags.map((value) => ({
      group: "SE outcome",
      label: humanizeToken(value),
      clear: () => updateQuery({ seRankingOutcomeFlags: query.seRankingOutcomeFlags.filter((item) => item !== value) }),
    })),
    ...(query.hasSiteStatusCheck
      ? [{ group: "Site status", label: "Checked only", clear: () => updateQuery({ hasSiteStatusCheck: false }) }]
      : []),
    ...query.siteStatusCategories.map((value) => ({
      group: "Site status",
      label: humanizeToken(value),
      clear: () => updateQuery({ siteStatusCategories: query.siteStatusCategories.filter((item) => item !== value) }),
    })),
    ...(query.hasScreamingFrogAudit
      ? [{ group: "Screaming Frog", label: "Audited only", clear: () => updateQuery({ hasScreamingFrogAudit: false }) }]
      : []),
    ...query.screamingFrogStatuses.map((value) => ({
      group: "Screaming Frog",
      label: humanizeToken(value),
      clear: () => updateQuery({ screamingFrogStatuses: query.screamingFrogStatuses.filter((item) => item !== value) }),
    })),
    ...query.screamingFrogHomepageStatuses.map((value) => ({
      group: "SF homepage",
      label: humanizeToken(value),
      clear: () => updateQuery({ screamingFrogHomepageStatuses: query.screamingFrogHomepageStatuses.filter((item) => item !== value) }),
    })),
    ...query.screamingFrogTitleFlags.map((value) => ({
      group: "SF titles",
      label: humanizeToken(value),
      clear: () => updateQuery({ screamingFrogTitleFlags: query.screamingFrogTitleFlags.filter((item) => item !== value) }),
    })),
    ...query.screamingFrogMetaFlags.map((value) => ({
      group: "SF meta",
      label: humanizeToken(value),
      clear: () => updateQuery({ screamingFrogMetaFlags: query.screamingFrogMetaFlags.filter((item) => item !== value) }),
    })),
    ...query.screamingFrogCanonicalFlags.map((value) => ({
      group: "SF canonicals",
      label: humanizeToken(value),
      clear: () => updateQuery({ screamingFrogCanonicalFlags: query.screamingFrogCanonicalFlags.filter((item) => item !== value) }),
    })),
    ...(query.hasScreamingFrogInternalErrors
      ? [{ group: "Screaming Frog", label: "Internal errors", clear: () => updateQuery({ hasScreamingFrogInternalErrors: false }) }]
      : []),
    ...(query.hasScreamingFrogLocationPages
      ? [{ group: "Screaming Frog", label: "Location pages", clear: () => updateQuery({ hasScreamingFrogLocationPages: false }) }]
      : []),
    ...(query.hasScreamingFrogServicePages
      ? [{ group: "Screaming Frog", label: "Service pages", clear: () => updateQuery({ hasScreamingFrogServicePages: false }) }]
      : []),
    ...query.timelinePlatforms.map((value) => ({
      group: "Timeline CMS",
      label: humanizeToken(value),
      clear: () => updateQuery({ timelinePlatforms: query.timelinePlatforms.filter((item) => item !== value) }),
    })),
    ...(!isDefaultTimelineEventSelection(query.timelineEventTypes)
      ? query.timelineEventTypes.map((value) => ({
          group: "Event",
          label: TIMELINE_EVENT_LABELS[value],
          clear: () => updateQuery({ timelineEventTypes: query.timelineEventTypes.filter((item) => item !== value) as TimelineEventType[] }),
        }))
      : []),
    ...(hasTimelineSelection && query.timelineDateField !== "first_seen"
      ? [{ group: "Platform signals", label: "Last seen", clear: () => updateQuery({ timelineDateField: "first_seen" }) }]
      : []),
    ...(hasTimelineSelection && query.timelineSeenFrom
      ? [{ group: "Platform signals", label: `From ${query.timelineSeenFrom}`, clear: () => updateQuery({ timelineSeenFrom: "" }) }]
      : []),
    ...(hasTimelineSelection && query.timelineSeenTo
      ? [{ group: "Platform signals", label: `To ${query.timelineSeenTo}`, clear: () => updateQuery({ timelineSeenTo: "" }) }]
      : []),
    ...(hasTimelineSelection && query.timelineGranularity !== "month"
      ? [{ group: "Platform signals", label: humanizeToken(query.timelineGranularity), clear: () => updateQuery({ timelineGranularity: "month" }) }]
      : []),
    ...(query.cmsMigrationFrom
      ? [{ group: "CMS migration", label: `From ${query.cmsMigrationFrom}`, clear: () => updateQuery({ cmsMigrationFrom: "" }) }]
      : []),
    ...(query.cmsMigrationTo
      ? [{ group: "CMS migration", label: `To ${query.cmsMigrationTo}`, clear: () => updateQuery({ cmsMigrationTo: "" }) }]
      : []),
    ...(query.cmsUnchangedYears
      ? [{ group: "CMS unchanged", label: `${query.cmsUnchangedYears}+ years`, clear: () => updateQuery({ cmsUnchangedYears: "" }) }]
      : []),
    ...(query.domainMigrationFrom
      ? [{ group: "Domain migration", label: `From ${query.domainMigrationFrom}`, clear: () => updateQuery({ domainMigrationFrom: "" }) }]
      : []),
    ...(query.domainMigrationTo
      ? [{ group: "Domain migration", label: `To ${query.domainMigrationTo}`, clear: () => updateQuery({ domainMigrationTo: "" }) }]
      : []),
    ...(hasCmsTiming && hasDomainTiming
      ? [{
          group: "Migration logic",
          label: migrationTimingLogicLabel,
          clear: () => updateQuery({ migrationTimingOperator: "and" }),
        }]
      : []),
    ...query.salesBuckets.map((value) => ({
      group: "Bucket",
      label: humanizeToken(value),
      clear: () => updateQuery({ salesBuckets: query.salesBuckets.filter((item) => item !== value) }),
    })),
    ...query.verticals.map((value) => ({
      group: "Vertical",
      label: value,
      clear: () => updateQuery({ verticals: query.verticals.filter((item) => item !== value) }),
    })),
    ...(options && query.countries.length !== options.countries.length
      ? query.countries.map((value) => ({
          group: "Country",
          label: humanizeToken(value),
          clear: () => updateQuery({ countries: query.countries.filter((item) => item !== value) }),
        }))
      : []),
    ...(options && query.tiers.length !== options.tiers.length
      ? query.tiers.map((value) => ({
          group: "Tier",
          label: value,
          clear: () => updateQuery({ tiers: query.tiers.filter((item) => item !== value) }),
        }))
      : []),
    ...(query.migrationOnly
      ? [{ group: "Flag", label: "Migration only", clear: () => updateQuery({ migrationOnly: false }) }]
      : []),
    ...(query.liveSitesOnly
      ? [{ group: "Flag", label: "Live sites only", clear: () => updateQuery({ liveSitesOnly: false }) }]
      : []),
    ...(query.hasDomainMigration
      ? [{ group: "Flag", label: "Has previous domain", clear: () => updateQuery({ hasDomainMigration: false }) }]
      : []),
    ...(query.hasCmsMigration
      ? [{ group: "Flag", label: "Has CMS migration", clear: () => updateQuery({ hasCmsMigration: false }) }]
      : []),
    ...(query.hasContact ? [{ group: "Flag", label: "Has contact", clear: () => updateQuery({ hasContact: false }) }] : []),
    ...(query.hasMarketing
      ? [{ group: "Flag", label: "Has marketing", clear: () => updateQuery({ hasMarketing: false }) }]
      : []),
    ...(query.hasCrm ? [{ group: "Flag", label: "Has CRM", clear: () => updateQuery({ hasCrm: false }) }] : []),
    ...(query.hasPayments
      ? [{ group: "Flag", label: "Has payments", clear: () => updateQuery({ hasPayments: false }) }]
      : []),
    ...(query.selectedOnly
      ? [{ group: "Flag", label: "Selected only", clear: () => updateQuery({ selectedOnly: false }) }]
      : []),
  ], [hasCmsTiming, hasDomainTiming, hasTimelineSelection, migrationTimingLogicLabel, options, query, updateQuery]);

  useEffect(() => {
    let cancelled = false;
    async function loadBoot() {
      try {
        const [healthResponse, summaryResponse, presetsResponse, trayResponse] = await Promise.all([
          fetchHealth(),
          fetchSummary(),
          fetchPresets(),
          fetchExportTray(),
        ]);
        if (!cancelled) {
          setBackendHealth(healthResponse);
          setBackendState(healthResponse.status === "ok" ? "connected" : "degraded");
          setSummary(summaryResponse);
          setPresets(presetsResponse.items);
          setTray(trayResponse);
          setError("");
        }
      } catch (loadError) {
        if (!cancelled) {
          setBackendState("offline");
          setError(loadError instanceof Error ? loadError.message : "Failed to load app");
        }
      }
    }
    void loadBoot();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;

    async function refreshHealth() {
      try {
        const response = await fetchHealth();
        if (cancelled) {
          return;
        }
        setBackendHealth(response);
        setBackendState(response.status === "ok" ? "connected" : "degraded");
      } catch {
        if (!cancelled) {
          setBackendState("offline");
        }
      }
    }

    void refreshHealth();
    const intervalId = window.setInterval(() => {
      void refreshHealth();
    }, 15000);

    return () => {
      cancelled = true;
      window.clearInterval(intervalId);
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    async function loadFilterOptions() {
      try {
        const optionsResponse = await fetchFilterOptions(scopedRequestQuery);
        if (!cancelled) {
          setOptions(optionsResponse);
        }
      } catch (loadError) {
        if (!cancelled) {
          setError(loadError instanceof Error ? loadError.message : "Failed to load filter options");
        }
      }
    }
    void loadFilterOptions();
    return () => {
      cancelled = true;
    };
  }, [scopedRequestQuery]);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setLeadTableState(leads?.items.length ? "retrying" : "loading");
    async function loadLeadsOnly() {
      try {
        const leadResponse = await fetchLeads(leadsRequestQuery);
        if (cancelled) {
          return;
        }
        setLeads(leadResponse);
        setLeadTableState(leadResponse.items.length ? "ready" : "empty");
        setBackendState("connected");
        setError("");

        if (drawerPending) {
          const candidate = drawerPending === "first" ? leadResponse.items[0] : leadResponse.items.at(-1);
          setSelectedLeadId(candidate?.root_domain ?? null);
          setDrawerPending(null);
          return;
        }

        if (selectedLeadId) {
          const exists = leadResponse.items.some((item) => item.root_domain === selectedLeadId);
          if (!exists) {
            setSelectedLeadId(null);
            setDetail(null);
          }
        }
      } catch (loadError) {
        if (!cancelled) {
          const message = loadError instanceof Error ? loadError.message : "Failed to load leads";
          const normalized = message.toLowerCase();
          const retryable = normalized.includes("timed out") || normalized.includes("503") || normalized.includes("500");
          setBackendState(retryable ? "degraded" : "offline");
          setLeadTableState(leads?.items.length ? "retrying" : "error");
          setError(
            retryable
              ? "Database busy or backend reconnecting. Retrying the worksheet while keeping the last successful results."
              : message,
          );
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }
    void loadLeadsOnly();
    return () => {
      cancelled = true;
    };
  }, [leadsRequestQuery, drawerPending]);

  useEffect(() => {
    let cancelled = false;
    if (!selectedLeadId) {
      setDetail(null);
      return () => {
        cancelled = true;
      };
    }
    const leadId: string = selectedLeadId;
    setDetailLoading(true);
    async function loadDetail() {
      try {
        const response = await fetchLeadDetail(leadId);
        if (!cancelled) {
          setDetail(response);
        }
      } catch (loadError) {
        if (!cancelled) {
          setError(loadError instanceof Error ? loadError.message : "Failed to load lead detail");
        }
      } finally {
        if (!cancelled) {
          setDetailLoading(false);
        }
      }
    }
    void loadDetail();
    return () => {
      cancelled = true;
    };
  }, [selectedLeadId]);

  async function refreshPresets() {
    const response = await fetchPresets();
    setPresets(response.items);
  }

  async function refreshTray() {
    const response = await fetchExportTray();
    setTray(response);
  }

  function applyPreset(preset: Preset) {
    setCurrentPresetId(preset.id);
    setVisibleColumns(ensureColumns(preset.visibleColumns));
    setQuery(
      normalizeLeadQuery({
        ...preset.filters,
        page: 1,
        sortBy: preset.sort.sortBy,
        sortDirection: preset.sort.sortDirection,
      }),
    );
  }

  async function handleSavePreset() {
    setPresetNameDraft(activePreset?.isBuiltin ? "" : activePreset?.name ?? "");
    setPresetModal({ mode: "save" });
  }

  async function confirmSavePreset() {
    const name = presetNameDraft.trim();
    if (!name) {
      setError("Preset name is required");
      return;
    }
    try {
      await createPreset({
        name,
        filters: normalizeLeadQuery({ ...query, page: 1 }),
        visibleColumns,
        sort: { sortBy: query.sortBy, sortDirection: query.sortDirection },
      });
      setPresetModal(null);
      setPresetNameDraft("");
      await refreshPresets();
      setToast("Preset saved");
    } catch (saveError) {
      setError(saveError instanceof Error ? saveError.message : "Failed to save preset");
    }
  }

  async function confirmDeletePreset() {
    if (!activePreset || activePreset.isBuiltin) {
      return;
    }
    try {
      await deletePreset(activePreset.id);
      setCurrentPresetId(null);
      setPresetModal(null);
      await refreshPresets();
      setToast("Preset deleted");
    } catch (deleteError) {
      setError(deleteError instanceof Error ? deleteError.message : "Failed to delete preset");
    }
  }

  async function toggleTrayForLead(lead: Lead) {
    try {
      if (traySet.has(lead.root_domain)) {
        setTray(await removeFromExportTray(lead.root_domain));
      } else {
        setTray(await addToExportTray([lead.root_domain]));
      }
    } catch (trayError) {
      setError(trayError instanceof Error ? trayError.message : "Failed to update export tray");
    }
  }

  async function toggleTrayForPage() {
    if (!leads?.items.length) {
      return;
    }
    const pageDomains = leads.items.map((item) => item.root_domain);
    const allSelected = pageDomains.every((domain) => traySet.has(domain));
    try {
      if (allSelected) {
        for (const domain of pageDomains) {
          if (traySet.has(domain)) {
            await removeFromExportTray(domain);
          }
        }
        await refreshTray();
      } else {
        const missing = pageDomains.filter((domain) => !traySet.has(domain));
        setTray(await addToExportTray(missing));
      }
    } catch (trayError) {
      setError(trayError instanceof Error ? trayError.message : "Failed to update page selection");
    }
  }

  async function selectAllFilteredLeads() {
    setSelectionLoading(true);
    try {
      const response = await addFilteredToExportTray({ ...query, page: 1 });
      setTray(response);
      const matchedCount = response.matchedCount ?? response.count;
      const addedCount = response.addedCount ?? 0;
      setToast(addedCount > 0 ? `Added ${addedCount} of ${matchedCount} filtered leads to the tray` : `All ${matchedCount} filtered leads were already in the tray`);
    } catch (trayError) {
      setError(trayError instanceof Error ? trayError.message : "Failed to select filtered leads");
    } finally {
      setSelectionLoading(false);
    }
  }

  async function clearTraySelection() {
    try {
      setTray(await clearExportTray());
      setManualSePreview(null);
      setSeRankingSummary(null);
      setSiteStatusSummary(null);
      setScreamingFrogSummary(null);
      setScreamingFrogJobBatch(null);
      setScreamingFrogRecentResults([]);
      setManualSeFirstMonth("");
      setManualSeSecondMonth("");
      setSelectedLeadId(null);
      setDetail(null);
      if (query.selectedOnly) {
        updateQuery({ selectedOnly: false }, true);
      }
      setToast("Cleared all selected leads");
    } catch (trayError) {
      setError(trayError instanceof Error ? trayError.message : "Failed to clear tray");
    }
  }

  function toggleColumn(column: ColumnKey) {
    setVisibleColumns((current) =>
      current.includes(column) ? current.filter((item) => item !== column) : [...current, column],
    );
    setCurrentPresetId(null);
  }

  function setSort(sortBy: string) {
    const nextDirection = query.sortBy === sortBy && query.sortDirection === "desc" ? "asc" : "desc";
    updateQuery({ sortBy, sortDirection: nextDirection, page: 1 });
  }

  function setExplicitSort(sortBy: string, sortDirection: "asc" | "desc") {
    updateQuery({ sortBy, sortDirection, page: 1 });
  }

  function navigateDrawer(direction: "next" | "prev") {
    if (!selectedLead || !leads) {
      return;
    }
    const index = leads.items.findIndex((item) => item.root_domain === selectedLead.root_domain);
    if (index === -1) {
      return;
    }
    if (direction === "next") {
      const next = leads.items[index + 1];
      if (next) {
        setSelectedLeadId(next.root_domain);
      } else if (leads.page < leads.pages) {
        setDrawerPending("first");
        updateQuery({ page: leads.page + 1 }, true);
      }
      return;
    }
    const previous = leads.items[index - 1];
    if (previous) {
      setSelectedLeadId(previous.root_domain);
    } else if (leads.page > 1) {
      setDrawerPending("last");
      updateQuery({ page: leads.page - 1 }, true);
    }
  }

  async function copyToClipboard(value: string, label: string) {
    if (!value) {
      return;
    }
    try {
      await navigator.clipboard.writeText(value);
      setToast(`${label} copied`);
    } catch {
      setError(`Could not copy ${label}`);
    }
  }

  function openScreamingFrogAudit(rootDomain: string) {
    if (!rootDomain) {
      return;
    }
    window.open(screamingFrogAuditViewerUrl(rootDomain), "_blank", "noopener,noreferrer");
  }

  function exportSingleLead(detailResponse: LeadDetailResponse) {
    const values = {
      root_domain: detailResponse.exportReady.root_domain,
      company: detailResponse.exportReady.company,
      country: detailResponse.exportReady.country,
      best_old_domain: detailResponse.lead.best_old_domain,
      domain_migration_confidence: detailResponse.lead.domain_migration_confidence_band,
      cms_migration: detailResponse.lead.cms_migration_summary,
      se_analysis_mode: detailResponse.lead.se_ranking_analysis_mode,
      se_market: detailResponse.lead.se_ranking_market,
      se_first_month: detailResponse.lead.se_ranking_date_label_first || detailResponse.lead.se_ranking_first_month,
      se_second_month: detailResponse.lead.se_ranking_date_label_second || detailResponse.lead.se_ranking_second_month,
      se_traffic_before: detailResponse.lead.se_ranking_traffic_before,
      se_traffic_last_month: detailResponse.lead.se_ranking_traffic_last_month,
      se_traffic_delta_percent: detailResponse.lead.se_ranking_traffic_delta_percent,
      se_keywords_before: detailResponse.lead.se_ranking_keywords_before,
      se_keywords_last_month: detailResponse.lead.se_ranking_keywords_last_month,
      se_keywords_delta_percent: detailResponse.lead.se_ranking_keywords_delta_percent,
      se_outcome_flags: detailResponse.lead.se_ranking_outcome_flags.join(" | "),
      site_status: detailResponse.lead.site_status_category,
      site_status_code: detailResponse.lead.site_status_code,
      site_final_url: detailResponse.lead.site_status_final_url,
      site_checked_at: detailResponse.lead.site_status_checked_at,
      site_status_error: detailResponse.lead.site_status_error,
      emails: detailResponse.exportReady.emails.join(" | "),
      telephones: detailResponse.exportReady.telephones.join(" | "),
      people: detailResponse.exportReady.people.join(" | "),
      bucket_reasons: detailResponse.exportReady.bucket_reasons.join(" || "),
    };
    const header = Object.keys(values).join(",");
    const row = Object.values(values)
      .map((value) => `"${String(value).replaceAll('"', '""')}"`)
      .join(",");
    const blob = new Blob([`${header}\n${row}\n`], { type: "text/csv;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = `${detailResponse.exportReady.root_domain}.csv`;
    anchor.click();
    URL.revokeObjectURL(url);
  }

  function applySearchDraft() {
    updateQuery({ search: searchDraft.trim() });
  }

  async function toggleBrowserFullscreen() {
    try {
      if (document.fullscreenElement) {
        await document.exitFullscreen();
        return;
      }
      setSpreadsheetFocusMode(true);
      await document.documentElement.requestFullscreen();
    } catch {
      setError("Could not toggle browser full screen");
    }
  }

  function toggleSidebarVisibility() {
    if (spreadsheetFocusMode && effectiveSidebarCollapsed) {
      setSpreadsheetFocusMode(false);
      setSidebarCollapsed(false);
      return;
    }
    setSidebarCollapsed((current) => !current);
  }

  function toggleTrayVisibility() {
    if (spreadsheetFocusMode && effectiveTrayCollapsed) {
      setSpreadsheetFocusMode(false);
      setTrayCollapsed(false);
      return;
    }
    setTrayCollapsed((current) => !current);
  }

  function toggleSidebarSection(section: keyof typeof sidebarSections) {
    setSidebarSections((current) => ({ ...current, [section]: !current[section] }));
  }

  function clearCmsMigrationTiming() {
    updateQuery({ cmsMigrationFrom: "", cmsMigrationTo: "" });
  }

  function clearCmsUnchangedYears() {
    updateQuery({ cmsUnchangedYears: "" });
  }

  function clearDomainMigrationTiming() {
    updateQuery({ domainMigrationFrom: "", domainMigrationTo: "" });
  }

  function clearAllMigrationTiming() {
    updateQuery({
      cmsMigrationFrom: "",
      cmsMigrationTo: "",
      cmsUnchangedYears: "",
      domainMigrationFrom: "",
      domainMigrationTo: "",
      migrationTimingOperator: "and",
    });
  }

  function applyMigrationPreset(target: "cms" | "domain", months: number) {
    const window = migrationPresetWindow(months);
    if (target === "cms") {
      updateQuery({ cmsMigrationFrom: window.from, cmsMigrationTo: window.to });
      return;
    }
    updateQuery({ domainMigrationFrom: window.from, domainMigrationTo: window.to });
  }

  async function reloadWorksheet() {
    await refreshTray();
    const [summary, siteSummary] = await Promise.all([
      fetchSeRankingSummary(seRankingType, query, true),
      fetchSiteStatusSummary(),
    ]);
    setSeRankingSummary(summary);
    setSiteStatusSummary(siteSummary);
    setQuery((current) => normalizeLeadQuery({ ...current }));
    if (selectedLeadId) {
      setDetail(await fetchLeadDetail(selectedLeadId));
    }
  }

  async function handleRunSeRankingAnalysis(confirm = false) {
    try {
      setSeRankingLoading(true);
      const response = await runSeRankingAnalysis(seRankingType, confirm, query, true);
      setSeRankingSummary({ analysisType: response.analysisType, summary: response.summary });
      setSeRankingSummaryDirty(false);
      if (confirm) {
        await reloadWorksheet();
        setVisibleColumns((current) =>
          ensureColumns([
            ...current,
            "se_market",
            "se_traffic_change",
            "se_keyword_change",
            "se_outcome",
            "se_checked",
          ]),
        );
        setToast(`SE Ranking analyzed ${response.results.length} domains`);
      }
    } catch (runError) {
      setError(runError instanceof Error ? runError.message : "Failed to run SE Ranking analysis");
    } finally {
      setSeRankingLoading(false);
    }
  }

  async function handleRefreshSeRankingAnalysis() {
    try {
      setSeRankingLoading(true);
      const response = await refreshSeRankingAnalysis(seRankingType, query, true);
      setSeRankingSummary({ analysisType: response.analysisType, summary: response.summary });
      setSeRankingSummaryDirty(false);
      await reloadWorksheet();
      setToast(`SE Ranking refreshed ${response.results.length} domains`);
    } catch (refreshError) {
      setError(refreshError instanceof Error ? refreshError.message : "Failed to refresh SE Ranking analysis");
    } finally {
      setSeRankingLoading(false);
    }
  }

  async function handlePreviewManualSeRankingAnalysis() {
    try {
      setManualSeLoading(true);
      const response = await previewManualSeRankingAnalysis({
        firstMonth: manualSeFirstMonth,
        secondMonth: manualSeSecondMonth,
        useSelectedTray: true,
      });
      setManualSePreview(response);
    } catch (previewError) {
      setError(previewError instanceof Error ? previewError.message : "Failed to preview manual SE Ranking analysis");
    } finally {
      setManualSeLoading(false);
    }
  }

  async function handleRunManualSeRankingAnalysis() {
    try {
      setManualSeLoading(true);
      const response = await runManualSeRankingAnalysis({
        firstMonth: manualSeFirstMonth,
        secondMonth: manualSeSecondMonth,
        useSelectedTray: true,
      });
      setManualSePreview({
        analysisType: response.analysisType,
        analysisMode: response.analysisMode,
        firstMonth: response.firstMonth,
        secondMonth: response.secondMonth,
        summary: response.summary,
      });
      await reloadWorksheet();
      setVisibleColumns((current) =>
        ensureColumns([
          ...current,
          "se_market",
          "se_traffic_before",
          "se_traffic_last_month",
          "se_traffic_change",
          "se_keyword_change",
          "se_checked",
        ]),
      );
      setToast(`SE Ranking analyzed ${response.results.length} domains`);
    } catch (runError) {
      setError(runError instanceof Error ? runError.message : "Failed to run manual SE Ranking analysis");
    } finally {
      setManualSeLoading(false);
    }
  }

  async function handleRunSiteStatusCheck(confirm = false) {
    try {
      setSiteStatusLoading(true);
      const response = await runSiteStatusCheck(confirm);
      setSiteStatusSummary(response);
      if (confirm) {
        await reloadWorksheet();
        setVisibleColumns((current) => ensureColumns([...current, "site_status", "site_status_code", "site_checked"]));
        setToast(`Checked ${response.results.length} domains`);
      }
    } catch (runError) {
      setError(runError instanceof Error ? runError.message : "Failed to run site status checks");
    } finally {
      setSiteStatusLoading(false);
    }
  }

  async function handleRefreshSiteStatusCheck() {
    try {
      setSiteStatusLoading(true);
      const response = await refreshSiteStatusCheck();
      setSiteStatusSummary(response);
      await reloadWorksheet();
      setToast(`Refreshed ${response.results.length} site checks`);
    } catch (refreshError) {
      setError(refreshError instanceof Error ? refreshError.message : "Failed to refresh site status checks");
    } finally {
      setSiteStatusLoading(false);
    }
  }

  async function handleRunScreamingFrogAudit(confirm = false) {
    try {
      setScreamingFrogLoading(true);
      setScreamingFrogRunStatus({
        tone: "warning",
        label: confirm ? "Audit running" : "Preview loading",
        message: confirm
          ? "Screaming Frog has started locally on this Mac. Keep this page open while the crawl runs."
          : "Checking the selected tray leads and preparing the local crawl plan.",
      });
      const response = await runScreamingFrogAudit(screamingFrogCrawlMode, confirm);
      setScreamingFrogSummary(response);
      setScreamingFrogJobBatch(response.jobBatch ?? null);
      setScreamingFrogRecentResults(response.results);
      if (confirm) {
        setVisibleColumns((current) =>
          ensureColumns([
            ...current,
            "sf_status",
            "sf_config",
            "sf_quality",
            "sf_score",
            "sf_primary_issue",
            "sf_collection_title_signal",
            "sf_collection_intro",
            "sf_homepage_status",
            "sf_internal_errors",
            "sf_checked",
          ]),
        );
        setScreamingFrogRunStatus({
          tone: "warning",
          label: "Audit queued",
          message: response.jobBatch?.isActive
            ? `Queued ${response.jobBatch.items.length} local crawl${response.jobBatch.items.length === 1 ? "" : "s"}. Results will update here while Screaming Frog runs.`
            : `Saved ${response.results.length} local audits.`,
        });
        setToast(response.jobBatch?.isActive ? `Queued ${response.jobBatch.items.length} Screaming Frog audits` : `Screaming Frog audited ${response.results.length} domains`);
      } else {
        setScreamingFrogRunStatus({
          tone: "neutral",
          label: "Preview ready",
          message: `${response.summary.toRunCount} selected leads are ready for a local Screaming Frog audit.`,
        });
      }
    } catch (runError) {
      const message = runError instanceof Error ? runError.message : "Failed to run Screaming Frog audit";
      setScreamingFrogRunStatus({
        tone: "warning",
        label: "Audit failed",
        message,
      });
      setError(message);
    } finally {
      setScreamingFrogLoading(false);
    }
  }

  async function handleRefreshScreamingFrogAudit() {
    try {
      setScreamingFrogLoading(true);
      setScreamingFrogRunStatus({
        tone: "warning",
        label: "Refresh running",
        message: "Refreshing saved local Screaming Frog audits for the selected tray leads.",
      });
      const response = await refreshScreamingFrogAudit(screamingFrogCrawlMode);
      setScreamingFrogSummary(response);
      setScreamingFrogJobBatch(response.jobBatch ?? null);
      setScreamingFrogRecentResults(response.results);
      setScreamingFrogRunStatus({
        tone: "warning",
        label: response.jobBatch?.isActive ? "Refresh queued" : "Refresh complete",
        message: response.jobBatch?.isActive
          ? `Queued ${response.jobBatch.items.length} audit refresh${response.jobBatch.items.length === 1 ? "" : "es"}. Updated results will appear here as they finish.`
          : `Refreshed ${response.results.length} local audits.`,
      });
      setToast(response.jobBatch?.isActive ? `Queued ${response.jobBatch.items.length} Screaming Frog refreshes` : `Screaming Frog refreshed ${response.results.length} audits`);
    } catch (refreshError) {
      const message = refreshError instanceof Error ? refreshError.message : "Failed to refresh Screaming Frog audit";
      setScreamingFrogRunStatus({
        tone: "warning",
        label: "Refresh failed",
        message,
      });
      setError(message);
    } finally {
      setScreamingFrogLoading(false);
    }
  }

  async function handleStopScreamingFrogAudit() {
    if (!screamingFrogJobBatch?.batchId) {
      return;
    }
    try {
      setScreamingFrogLoading(true);
      const batch = await stopScreamingFrogJobBatch(screamingFrogJobBatch.batchId);
      setScreamingFrogJobBatch(batch);
      setScreamingFrogRunStatus({
        tone: "warning",
        label: "Crawl stopped",
        message: "Queued jobs were cancelled and the current local Screaming Frog run was asked to stop.",
      });
      await reloadWorksheet();
      const refreshed = await fetchScreamingFrogSummary(screamingFrogCrawlMode);
      setScreamingFrogSummary(refreshed);
      setScreamingFrogJobBatch(refreshed.jobBatch ?? batch);
      setToast("Stopped Screaming Frog crawl batch");
    } catch (stopError) {
      setError(stopError instanceof Error ? stopError.message : "Failed to stop Screaming Frog crawl batch");
    } finally {
      setScreamingFrogLoading(false);
    }
  }

  const hasMigrationTiming = hasCmsTiming || hasDomainTiming;
  const hasCmsUnchangedYears = Boolean(query.cmsUnchangedYears);
  const cmsTimingSummary = hasCmsTiming
    ? dateWindowLabel(query.cmsMigrationFrom, query.cmsMigrationTo)
    : "All CMS migration dates";
  const cmsUnchangedSummary = hasCmsUnchangedYears
    ? `No CMS change in ${query.cmsUnchangedYears}+ years`
    : "Find stale unchanged CMS";
  const domainTimingSummary = hasDomainTiming
    ? dateWindowLabel(query.domainMigrationFrom, query.domainMigrationTo)
    : "All domain migration dates";
  const migrationCaptureLabel = hasMigrationTiming ? migrationTimingLogicLabel : "Capture all migrations";
  const seEligibilityLabel = seRankingSummary
    ? `${seRankingSummary.summary.eligibleCount} displayed leads can run SE checks`
    : "Set your worksheet filters, then preview the displayed leads before running SE Ranking";
  const siteStatusLabel = siteStatusSummary
    ? `${siteStatusSummary.summary.toRunCount} selected leads still need a site check`
    : "Select tray leads to preview bulk site status checks";
  const screamingFrogLabel = screamingFrogSummary
    ? `${screamingFrogSummary.summary.toRunCount} selected leads still need a Screaming Frog audit`
    : "Select tray leads to preview local Screaming Frog audits";
  const screamingFrogBreakdown = screamingFrogSummary?.summary.resolvedConfigBreakdown
    ?.map((item) => `${item.label}: ${item.count}`)
    .join(" · ");
  const screamingFrogJobCounts = screamingFrogJobBatch?.counts ?? {};
  const selectedCmsCoverageWarnings = (summary?.source_coverage ?? [])
    .filter((item) => [...query.currentPlatforms, ...query.recentPlatforms, ...query.removedPlatforms].includes(item.platform) && !item.hasRemoved)
    .map((item) => humanizeToken(item.platform));
  const searchSectionCount = Number(Boolean(query.search || query.exactDomain));
  const commonSectionCount =
    query.verticals.length +
    (options && query.countries.length !== options.countries.length ? query.countries.length : 0) +
    (options && query.tiers.length !== options.tiers.length ? query.tiers.length : 0) +
    query.salesBuckets.length +
    query.currentPlatforms.length +
    query.removedPlatforms.length +
    query.recentPlatforms.length +
    Number(query.liveSitesOnly) +
    Number(query.hasDomainMigration) +
    Number(query.hasCmsMigration) +
    Number(query.hasContact) +
    Number(query.selectedOnly);
  const migrationSectionCount =
    query.domainMigrationStatuses.length +
    query.domainConfidenceBands.length +
    query.domainFingerprintStrengths.length +
    query.domainTldRelationships.length +
    query.cmsMigrationStatuses.length +
    query.cmsConfidenceLevels.length;
  const analysisSectionCount =
    Number(query.hasSeRankingAnalysis) +
    query.seRankingAnalysisTypes.length +
    query.seRankingOutcomeFlags.length +
    Number(query.hasSiteStatusCheck) +
    query.siteStatusCategories.length +
    Number(query.hasScreamingFrogAudit) +
    query.screamingFrogStatuses.length +
    query.screamingFrogHomepageStatuses.length +
    query.screamingFrogTitleFlags.length +
    query.screamingFrogMetaFlags.length +
    query.screamingFrogCanonicalFlags.length +
    Number(query.hasScreamingFrogInternalErrors) +
    Number(query.hasScreamingFrogLocationPages) +
    Number(query.hasScreamingFrogServicePages);
  const advancedSectionCount =
    Number(query.migrationOnly) +
    Number(query.hasMarketing) +
    Number(query.hasCrm) +
    Number(query.hasPayments) +
    query.marketingPlatforms.length +
    query.crmPlatforms.length +
    query.paymentPlatforms.length +
    query.hostingProviders.length +
    query.agencies.length +
    query.aiTools.length +
    query.complianceFlags.length +
    Number(Boolean(query.minSocial)) +
    Number(Boolean(query.minRevenue)) +
    Number(Boolean(query.minEmployees)) +
    Number(Boolean(query.minSku)) +
    Number(Boolean(query.minTechnologySpend));

  return (
    <div className={appShellClassName}>
      <header className="topbar">
        <div className="topbar-brand-block">
          <div className="brand-lockup">
            <div className="brand-copy">
              <p className="kicker">Migration intelligence for outbound</p>
              <h1>Lead Console</h1>
              <p className="intro">
                Cold-email list building across domains, platforms, and migration signals.
              </p>
            </div>
          </div>
          <div className="shell-pill-row">
            <span className="shell-pill">AU · NZ · SG</span>
            <span className="shell-pill">{activePreset?.name ?? "Current custom view"}</span>
            <span className="shell-pill">
              {loading ? "Refreshing worksheet" : `${(leads?.total ?? 0).toLocaleString()} leads in view`}
            </span>
            <span className={`shell-pill shell-pill-${backendPill.tone}`}>{backendPill.label}</span>
          </div>
        </div>
        <div className="command-bar">
          <div className="command-bar-main">
            <label className="field compact-field command-search-field">
              <span>Global search</span>
              <div className="search-field-row">
                <input
                  value={searchDraft}
                  onChange={(event) => setSearchDraft(event.target.value)}
                  onKeyDown={(event) => {
                    if (event.key === "Enter") {
                      event.preventDefault();
                      applySearchDraft();
                    }
                  }}
                  placeholder="Search domain, company, tool, or vertical"
                />
                <button
                  className="primary-button small-button"
                  disabled={searchDraft.trim() === query.search.trim()}
                  onClick={applySearchDraft}
                  type="button"
                >
                  Search
                </button>
              </div>
            </label>
            <label className="field compact-field preset-select-field command-preset-field">
              <span>Workspace view</span>
              <select
                value={currentPresetId ?? ""}
                onChange={(event) => {
                  const preset = presets.find((item) => item.id === event.target.value);
                  if (preset) {
                    applyPreset(preset);
                  } else {
                    setCurrentPresetId(null);
                  }
                }}
              >
                <option value="">Current custom view</option>
                {groupedPresets.map(([group, items]) => (
                  <optgroup label={group} key={group}>
                    {items.map((preset) => (
                      <option key={preset.id} value={preset.id}>
                        {preset.name}
                      </option>
                    ))}
                  </optgroup>
                ))}
              </select>
            </label>
            <div className="command-view-summary">
              <strong>{activePreset?.name ?? "Current custom view"}</strong>
              <span>{activePreset?.description ?? "Start broad, then refine from the rail and worksheet controls."}</span>
            </div>
          </div>
          <div className="header-actions">
            <a className="primary-button" href={exportLeadUrl(query)}>
              Export filtered CSV
            </a>
            <button className="ghost-button" onClick={handleSavePreset} type="button">
              Save preset
            </button>
            <button className="ghost-button" onClick={() => setShowGuideModal(true)} type="button">
              Guide
            </button>
            <div className="stamp-card">
              <span>Processed</span>
              <strong>{summary ? new Date(summary.processed_at).toLocaleString() : "Loading…"}</strong>
              <small>{summary ? `${summary.overview.unique_leads.toLocaleString()} scoped leads` : ""}</small>
            </div>
          </div>
        </div>
      </header>

      <main className={`workspace ${effectiveSidebarCollapsed ? "workspace-sidebar-collapsed" : ""}`}>
        <aside className={`filter-rail ${effectiveSidebarCollapsed ? "rail-collapsed" : ""}`}>
          <div className="rail-header">
            <h2>Filters</h2>
            <div className="rail-actions">
              {!effectiveSidebarCollapsed ? (
                <button
                  className="link-button"
                  onClick={() => {
                    setQuery(initialQuery);
                    setVisibleColumns(defaultVisibleColumns);
                    setCurrentPresetId(null);
                  }}
                  type="button"
                >
                  Reset
                </button>
              ) : null}
              <button className="ghost-button small-button" onClick={toggleSidebarVisibility} type="button">
                {effectiveSidebarCollapsed ? "Expand" : "Collapse"}
              </button>
            </div>
          </div>
          {effectiveSidebarCollapsed ? (
            <div className="rail-collapsed-body">
              <button className="ghost-button small-button" onClick={toggleSidebarVisibility} type="button">
                Show filters
              </button>
              <div className="rail-collapsed-stat">
                <strong>{activeFilterChips.length}</strong>
                <span>Active filters</span>
              </div>
              <div className="rail-collapsed-stat">
                <strong>{tray?.count ?? 0}</strong>
                <span>In tray</span>
              </div>
            </div>
          ) : (
            <>
              <SidebarSection
                title="Search"
                description="Lock onto a specific domain or review the current search context."
                activeCount={searchSectionCount}
                open={sidebarSections.search}
                onToggle={() => toggleSidebarSection("search")}
              >
                <label className="field">
                  <span>Current global search</span>
                  <input disabled value={query.search || "No global search applied"} />
                </label>
                <label className="field compact-field">
                  <span>Exact domain override</span>
                  <div className="search-field-row">
                    <input
                      value={exactDomainDraft}
                      onChange={(event) => setExactDomainDraft(event.target.value)}
                      placeholder="example.com.au"
                    />
                    <button
                      className="ghost-button small-button"
                      disabled={!query.search && !query.exactDomain}
                      onClick={() => updateQuery({ search: "", exactDomain: "" })}
                      type="button"
                    >
                      Clear
                    </button>
                  </div>
                </label>
              </SidebarSection>

              <SidebarSection
                title="Commonly used"
                description="Your main discovery filters for narrowing the lead universe quickly."
                activeCount={commonSectionCount}
                open={sidebarSections.common}
                onToggle={() => toggleSidebarSection("common")}
              >
                <section className="filter-block">
                  <div className="filter-header">
                    <h3>Verticals</h3>
                    <span className="muted">{query.verticals.length} selected</span>
                  </div>
                  {shouldShowVerticalSearch ? (
                    <label className="field compact-field">
                      <span>Search verticals</span>
                      <input
                        value={verticalSearch}
                        onChange={(event) => setVerticalSearch(event.target.value)}
                        placeholder="fashion, industrial, beauty"
                      />
                    </label>
                  ) : null}
                  <FilterBlock
                    title=""
                    items={filteredVerticalOptions.slice(0, 80)}
                    selected={query.verticals}
                    onToggle={(value) => updateQuery({ verticals: toggle(query.verticals, value) })}
                  />
                </section>

                <FilterBlock
                  title="Countries"
                  items={options?.countries ?? ["AU", "NZ", "SG"]}
                  selected={query.countries}
                  onToggle={(value) => updateQuery({ countries: toggle(query.countries, value) })}
                  formatLabel={humanizeToken}
                />

                <FilterBlock
                  title="Lead angles"
                  items={options?.salesBuckets ?? []}
                  selected={query.salesBuckets}
                  onToggle={(value) => updateQuery({ salesBuckets: toggle(query.salesBuckets, value) })}
                  formatLabel={humanizeToken}
                />

                <FilterBlock
                  title="Current platform"
                  items={options?.currentPlatforms ?? []}
                  selected={query.currentPlatforms}
                  onToggle={(value) => updateQuery({ currentPlatforms: toggle(query.currentPlatforms, value) })}
                  formatLabel={humanizeToken}
                  searchable
                />

                <FilterBlock
                  title="Previous platform seen"
                  items={options?.removedPlatforms ?? []}
                  selected={query.removedPlatforms}
                  onToggle={(value) => updateQuery({ removedPlatforms: toggle(query.removedPlatforms, value) })}
                  formatLabel={humanizeToken}
                  searchable
                />

                <FilterBlock
                  title="New platform seen"
                  items={options?.recentPlatforms ?? []}
                  selected={query.recentPlatforms}
                  onToggle={(value) => updateQuery({ recentPlatforms: toggle(query.recentPlatforms, value) })}
                  formatLabel={humanizeToken}
                  searchable
                />

                <FilterBlock
                  title="Priority tiers"
                  items={options?.tiers ?? ["A", "B", "C", "D"]}
                  selected={query.tiers}
                  onToggle={(value) => updateQuery({ tiers: toggle(query.tiers, value) })}
                />

                <div className="toggle-grid compact-toggle-grid">
                  <ToggleRow
                    label="Live sites only"
                    checked={query.liveSitesOnly}
                    onChange={() => updateQuery({ liveSitesOnly: !query.liveSitesOnly })}
                  />
                  <ToggleRow
                    label="Has previous domain"
                    checked={query.hasDomainMigration}
                    onChange={() => updateQuery({ hasDomainMigration: !query.hasDomainMigration })}
                  />
                  <ToggleRow
                    label="Has CMS migration"
                    checked={query.hasCmsMigration}
                    onChange={() => updateQuery({ hasCmsMigration: !query.hasCmsMigration })}
                  />
                  <ToggleRow
                    label="Has contact data"
                    checked={query.hasContact}
                    onChange={() => updateQuery({ hasContact: !query.hasContact })}
                  />
                  <ToggleRow
                    label="Selected tray only"
                    checked={query.selectedOnly}
                    onChange={() => updateQuery({ selectedOnly: !query.selectedOnly })}
                  />
                </div>
              </SidebarSection>

              <SidebarSection
                title="Migration quality"
                description="Qualify previous-domain and CMS confidence once you already have a shortlist."
                activeCount={migrationSectionCount}
                open={sidebarSections.migration}
                onToggle={() => toggleSidebarSection("migration")}
              >
                <FilterBlock
                  title="Previous domain status"
                  items={options?.domainMigrationStatuses ?? []}
                  selected={query.domainMigrationStatuses}
                  onToggle={(value) => updateQuery({ domainMigrationStatuses: toggle(query.domainMigrationStatuses, value) })}
                  formatLabel={humanizeToken}
                />

                <FilterBlock
                  title="Domain confidence"
                  items={options?.domainConfidenceBands ?? []}
                  selected={query.domainConfidenceBands}
                  onToggle={(value) => updateQuery({ domainConfidenceBands: toggle(query.domainConfidenceBands, value) })}
                  formatLabel={humanizeToken}
                />

                <FilterBlock
                  title="Fingerprint strength"
                  items={options?.domainFingerprintStrengths ?? []}
                  selected={query.domainFingerprintStrengths}
                  onToggle={(value) =>
                    updateQuery({ domainFingerprintStrengths: toggle(query.domainFingerprintStrengths, value) })
                  }
                  formatLabel={humanizeToken}
                />

                <FilterBlock
                  title="Domain TLD relationship"
                  items={options?.domainTldRelationships ?? []}
                  selected={query.domainTldRelationships}
                  onToggle={(value) => updateQuery({ domainTldRelationships: toggle(query.domainTldRelationships, value) })}
                  formatLabel={humanizeToken}
                />

                <FilterBlock
                  title="CMS migration status"
                  items={options?.cmsMigrationStatuses ?? []}
                  selected={query.cmsMigrationStatuses}
                  onToggle={(value) => updateQuery({ cmsMigrationStatuses: toggle(query.cmsMigrationStatuses, value) })}
                  formatLabel={humanizeToken}
                />

                <FilterBlock
                  title="CMS migration confidence"
                  items={options?.cmsConfidenceLevels ?? []}
                  selected={query.cmsConfidenceLevels}
                  onToggle={(value) => updateQuery({ cmsConfidenceLevels: toggle(query.cmsConfidenceLevels, value) })}
                  formatLabel={humanizeToken}
                />

              </SidebarSection>

              <SidebarSection
                title="Analysis & SEO"
                description="Use saved SEO analysis states once you move from discovery into validation."
                activeCount={analysisSectionCount}
                open={sidebarSections.analysis}
                onToggle={() => toggleSidebarSection("analysis")}
              >
                <div className="toggle-grid compact-toggle-grid">
                  <ToggleRow
                    label="Has SE Ranking analysis"
                    checked={query.hasSeRankingAnalysis}
                    onChange={() => updateQuery({ hasSeRankingAnalysis: !query.hasSeRankingAnalysis })}
                  />
                  <ToggleRow
                    label="Has site status check"
                    checked={query.hasSiteStatusCheck}
                    onChange={() => updateQuery({ hasSiteStatusCheck: !query.hasSiteStatusCheck })}
                  />
                  <ToggleRow
                    label="Has Screaming Frog audit"
                    checked={query.hasScreamingFrogAudit}
                    onChange={() => updateQuery({ hasScreamingFrogAudit: !query.hasScreamingFrogAudit })}
                  />
                  <ToggleRow
                    label="SF internal errors"
                    checked={query.hasScreamingFrogInternalErrors}
                    onChange={() => updateQuery({ hasScreamingFrogInternalErrors: !query.hasScreamingFrogInternalErrors })}
                  />
                  <ToggleRow
                    label="SF location pages"
                    checked={query.hasScreamingFrogLocationPages}
                    onChange={() => updateQuery({ hasScreamingFrogLocationPages: !query.hasScreamingFrogLocationPages })}
                  />
                  <ToggleRow
                    label="SF service pages"
                    checked={query.hasScreamingFrogServicePages}
                    onChange={() => updateQuery({ hasScreamingFrogServicePages: !query.hasScreamingFrogServicePages })}
                  />
                </div>
                <FilterBlock
                  title="SE Ranking analysis type"
                  items={options?.seRankingAnalysisTypes ?? []}
                  selected={query.seRankingAnalysisTypes}
                  onToggle={(value) => updateQuery({ seRankingAnalysisTypes: toggle(query.seRankingAnalysisTypes, value) })}
                  formatLabel={humanizeToken}
                />

                <FilterBlock
                  title="SE Ranking outcome"
                  items={options?.seRankingOutcomeFlags ?? []}
                  selected={query.seRankingOutcomeFlags}
                  onToggle={(value) => updateQuery({ seRankingOutcomeFlags: toggle(query.seRankingOutcomeFlags, value) })}
                  formatLabel={humanizeToken}
                />
                <FilterBlock
                  title="Site status"
                  items={options?.siteStatusCategories ?? []}
                  selected={query.siteStatusCategories}
                  onToggle={(value) => updateQuery({ siteStatusCategories: toggle(query.siteStatusCategories, value) })}
                  formatLabel={humanizeToken}
                />
                <FilterBlock
                  title="Screaming Frog status"
                  items={options?.screamingFrogStatuses ?? []}
                  selected={query.screamingFrogStatuses}
                  onToggle={(value) => updateQuery({ screamingFrogStatuses: toggle(query.screamingFrogStatuses, value) })}
                  formatLabel={humanizeToken}
                />
                <FilterBlock
                  title="SF homepage status"
                  items={options?.screamingFrogHomepageStatuses ?? []}
                  selected={query.screamingFrogHomepageStatuses}
                  onToggle={(value) => updateQuery({ screamingFrogHomepageStatuses: toggle(query.screamingFrogHomepageStatuses, value) })}
                  formatLabel={humanizeToken}
                />
                <FilterBlock
                  title="SF title issues"
                  items={options?.screamingFrogTitleFlags ?? []}
                  selected={query.screamingFrogTitleFlags}
                  onToggle={(value) => updateQuery({ screamingFrogTitleFlags: toggle(query.screamingFrogTitleFlags, value) })}
                  formatLabel={humanizeToken}
                />
                <FilterBlock
                  title="SF meta issues"
                  items={options?.screamingFrogMetaFlags ?? []}
                  selected={query.screamingFrogMetaFlags}
                  onToggle={(value) => updateQuery({ screamingFrogMetaFlags: toggle(query.screamingFrogMetaFlags, value) })}
                  formatLabel={humanizeToken}
                />
                <FilterBlock
                  title="SF canonical issues"
                  items={options?.screamingFrogCanonicalFlags ?? []}
                  selected={query.screamingFrogCanonicalFlags}
                  onToggle={(value) => updateQuery({ screamingFrogCanonicalFlags: toggle(query.screamingFrogCanonicalFlags, value) })}
                  formatLabel={humanizeToken}
                />
              </SidebarSection>

              <SidebarSection
                title="Advanced"
                description="Lower-frequency commercial and stack signals for deeper segmentation."
                activeCount={advancedSectionCount}
                open={sidebarSections.advanced}
                onToggle={() => toggleSidebarSection("advanced")}
              >
                <section className="filter-block">
                  <h3>Minimum values</h3>
                  <div className="advanced-number-grid">
                    <AdvancedNumberField label="Followers" value={query.minSocial} onChange={(value) => updateQuery({ minSocial: value })} placeholder="10000" />
                    <AdvancedNumberField label="Revenue" value={query.minRevenue} onChange={(value) => updateQuery({ minRevenue: value })} placeholder="500000" />
                    <AdvancedNumberField label="Employees" value={query.minEmployees} onChange={(value) => updateQuery({ minEmployees: value })} placeholder="10" />
                    <AdvancedNumberField label="SKU" value={query.minSku} onChange={(value) => updateQuery({ minSku: value })} placeholder="100" />
                    <AdvancedNumberField
                      label="Tech spend"
                      value={query.minTechnologySpend}
                      onChange={(value) => updateQuery({ minTechnologySpend: value })}
                      placeholder="1000"
                    />
                  </div>
                </section>

                <div className="toggle-grid compact-toggle-grid">
                  <ToggleRow
                    label="Has marketing stack"
                    checked={query.hasMarketing}
                    onChange={() => updateQuery({ hasMarketing: !query.hasMarketing })}
                  />
                  <ToggleRow label="Has CRM" checked={query.hasCrm} onChange={() => updateQuery({ hasCrm: !query.hasCrm })} />
                  <ToggleRow
                    label="Has payments"
                    checked={query.hasPayments}
                    onChange={() => updateQuery({ hasPayments: !query.hasPayments })}
                  />
                  <ToggleRow
                    label="Migration only"
                    checked={query.migrationOnly}
                    onChange={() => updateQuery({ migrationOnly: !query.migrationOnly })}
                  />
                </div>

                <FilterBlock
                  title="Marketing tools"
                  items={options?.marketingPlatforms ?? []}
                  selected={query.marketingPlatforms}
                  onToggle={(value) => updateQuery({ marketingPlatforms: toggle(query.marketingPlatforms, value) })}
                  onSelectAll={() => updateQuery({ marketingPlatforms: options?.marketingPlatforms ?? [] })}
                  onClearAll={() => updateQuery({ marketingPlatforms: [] })}
                  searchable
                />

                <FilterBlock
                  title="CRM tools"
                  items={options?.crmPlatforms ?? []}
                  selected={query.crmPlatforms}
                  onToggle={(value) => updateQuery({ crmPlatforms: toggle(query.crmPlatforms, value) })}
                  onSelectAll={() => updateQuery({ crmPlatforms: options?.crmPlatforms ?? [] })}
                  onClearAll={() => updateQuery({ crmPlatforms: [] })}
                  searchable
                />

                <FilterBlock
                  title="Payment tools"
                  items={options?.paymentPlatforms ?? []}
                  selected={query.paymentPlatforms}
                  onToggle={(value) => updateQuery({ paymentPlatforms: toggle(query.paymentPlatforms, value) })}
                  onSelectAll={() => updateQuery({ paymentPlatforms: options?.paymentPlatforms ?? [] })}
                  onClearAll={() => updateQuery({ paymentPlatforms: [] })}
                  searchable
                />

                <FilterBlock
                  title="Hosting providers"
                  items={options?.hostingProviders ?? []}
                  selected={query.hostingProviders}
                  onToggle={(value) => updateQuery({ hostingProviders: toggle(query.hostingProviders, value) })}
                  onSelectAll={() => updateQuery({ hostingProviders: options?.hostingProviders ?? [] })}
                  onClearAll={() => updateQuery({ hostingProviders: [] })}
                  searchable
                />

                <FilterBlock
                  title="Agencies"
                  items={options?.agencies ?? []}
                  selected={query.agencies}
                  onToggle={(value) => updateQuery({ agencies: toggle(query.agencies, value) })}
                  onSelectAll={() => updateQuery({ agencies: options?.agencies ?? [] })}
                  onClearAll={() => updateQuery({ agencies: [] })}
                  searchable
                />

                <FilterBlock
                  title="AI tools"
                  items={options?.aiTools ?? []}
                  selected={query.aiTools}
                  onToggle={(value) => updateQuery({ aiTools: toggle(query.aiTools, value) })}
                  onSelectAll={() => updateQuery({ aiTools: options?.aiTools ?? [] })}
                  onClearAll={() => updateQuery({ aiTools: [] })}
                  searchable
                />

                <FilterBlock
                  title="Compliance"
                  items={options?.complianceFlags ?? []}
                  selected={query.complianceFlags}
                  onToggle={(value) => updateQuery({ complianceFlags: toggle(query.complianceFlags, value) })}
                  onSelectAll={() => updateQuery({ complianceFlags: options?.complianceFlags ?? [] })}
                  onClearAll={() => updateQuery({ complianceFlags: [] })}
                  searchable
                />
              </SidebarSection>
            </>
          )}
        </aside>

        <section className="grid-panel">
          <div className="grid-toolbar">
            <div className="worksheet-title-block">
              <p className="kicker-inline">Worksheet</p>
              <h2>Lead worksheet</h2>
              <p>
                {loading ? "Refreshing…" : `${leads?.total ?? 0} matching leads`} · sorted by {sortBadge(query)}
              </p>
            </div>
            <div className="toolbar-actions">
              <button
                className="primary-button small-button"
                disabled={!leads?.total || selectionLoading}
                onClick={() => void selectAllFilteredLeads()}
                type="button"
              >
                {selectionLoading ? "Selecting…" : `Select all filtered${leads?.total ? ` (${leads.total.toLocaleString()})` : ""}`}
              </button>
              <button
                className="ghost-button small-button danger-button"
                disabled={!tray?.count}
                onClick={() => void clearTraySelection()}
                type="button"
              >
                Clear all selected
              </button>
              <button
                className={`ghost-button small-button ${effectiveSidebarCollapsed ? "timeline-toggle-active" : ""}`}
                onClick={toggleSidebarVisibility}
                type="button"
              >
                {effectiveSidebarCollapsed ? "Show filters" : "Hide filters"}
              </button>
              <button
                className={`ghost-button small-button ${spreadsheetFocusMode ? "timeline-toggle-active" : ""}`}
                onClick={() => setSpreadsheetFocusMode((current) => !current)}
                type="button"
              >
                {spreadsheetFocusMode ? "Exit focus" : "Focus mode"}
              </button>
              <button
                className={`ghost-button small-button ${browserFullscreen ? "timeline-toggle-active" : ""}`}
                onClick={() => void toggleBrowserFullscreen()}
                type="button"
              >
                {browserFullscreen ? "Exit full screen" : "Full screen"}
              </button>
              <button className="ghost-button small-button" onClick={() => setShowColumnChooser((open) => !open)} type="button">
                Columns
              </button>
              <label className="page-size">
                <span>Rows</span>
                <select value={query.pageSize} onChange={(event) => updateQuery({ pageSize: Number(event.target.value), page: 1 })}>
                  {pageSizeOptions.map((size) => (
                    <option key={size} value={size}>
                      {size}
                    </option>
                  ))}
                </select>
              </label>
              <div className="page-chip">
                Page {leads?.page ?? 1} of {leads?.pages ?? 1}
              </div>
              <div className="page-chip selection-chip">
                {(tray?.count ?? 0).toLocaleString()} selected
              </div>
            </div>
          </div>

          {showColumnChooser ? (
            <div className="column-chooser">
              <div className="column-chooser-header">
                <strong>Visible columns</strong>
                <span className="muted">Company and domain stay pinned.</span>
              </div>
              <div className="column-list">
                {Object.keys(columnLabels).map((column) => (
                  <label key={column}>
                    <input
                      checked={visibleColumns.includes(column as ColumnKey)}
                      onChange={() => toggleColumn(column as ColumnKey)}
                      type="checkbox"
                    />
                    <span>{columnLabels[column as ColumnKey]}</span>
                  </label>
                ))}
              </div>
            </div>
          ) : null}

          <div className="active-filter-row worksheet-chip-row">
            {activeFilterChips.map((chip) => (
              <button className="filter-chip" key={`${chip.group}-${chip.label}`} onClick={chip.clear} type="button">
                <span>{chip.group}</span>
                <strong>{chip.label}</strong>
                <small>×</small>
              </button>
            ))}
          </div>

          <div className="quick-toggle-row worksheet-toggle-row">
            <button
              className={`ghost-button small-button ${query.liveSitesOnly ? "timeline-toggle-active" : ""}`}
              onClick={() => updateQuery({ liveSitesOnly: !query.liveSitesOnly })}
              type="button"
            >
              Live sites only
            </button>
            <button
              className={`ghost-button small-button ${query.hasDomainMigration ? "timeline-toggle-active" : ""}`}
              onClick={() => updateQuery({ hasDomainMigration: !query.hasDomainMigration })}
              type="button"
            >
              Has previous domain
            </button>
            <button
              className={`ghost-button small-button ${query.hasCmsMigration ? "timeline-toggle-active" : ""}`}
              onClick={() => updateQuery({ hasCmsMigration: !query.hasCmsMigration })}
              type="button"
            >
              Has CMS migration
            </button>
            <button
              className={`ghost-button small-button ${query.domainMigrationStatuses.includes("confirmed") ? "timeline-toggle-active" : ""}`}
              onClick={() =>
                updateQuery({
                  domainMigrationStatuses: query.domainMigrationStatuses.includes("confirmed")
                    ? query.domainMigrationStatuses.filter((value) => value !== "confirmed")
                    : [...query.domainMigrationStatuses, "confirmed"],
                })
              }
              type="button"
            >
              Confirmed domain
            </button>
            <button
              className={`ghost-button small-button ${query.cmsMigrationStatuses.includes("confirmed") ? "timeline-toggle-active" : ""}`}
              onClick={() =>
                updateQuery({
                  cmsMigrationStatuses: query.cmsMigrationStatuses.includes("confirmed")
                    ? query.cmsMigrationStatuses.filter((value) => value !== "confirmed")
                    : [...query.cmsMigrationStatuses, "confirmed"],
                })
              }
              type="button"
            >
              Confirmed CMS
            </button>
            <button
              className={`ghost-button small-button ${query.domainFingerprintStrengths.includes("Strong") ? "timeline-toggle-active" : ""}`}
              onClick={() =>
                updateQuery({
                  domainFingerprintStrengths: query.domainFingerprintStrengths.includes("Strong")
                    ? query.domainFingerprintStrengths.filter((value) => value !== "Strong")
                    : [...query.domainFingerprintStrengths, "Strong"],
                })
              }
              type="button"
            >
              Strong fingerprint
            </button>
            <button
              className={`ghost-button small-button ${query.hasSeRankingAnalysis ? "timeline-toggle-active" : ""}`}
              onClick={() => updateQuery({ hasSeRankingAnalysis: !query.hasSeRankingAnalysis })}
              type="button"
            >
              SE analyzed
            </button>
            <button
              className={`ghost-button small-button ${query.hasSiteStatusCheck ? "timeline-toggle-active" : ""}`}
              onClick={() => updateQuery({ hasSiteStatusCheck: !query.hasSiteStatusCheck })}
              type="button"
            >
              Site checked
            </button>
          </div>

          <section className="worksheet-analysis-bar">
            <article className={`analysis-module migration-analysis-module ${hasMigrationTiming ? "migration-analysis-active" : ""}`}>
              <div className="migration-analysis-header">
                <div className="migration-analysis-summary">
                  <span className="kicker-inline">Migration analysis</span>
                  <strong>{migrationCaptureLabel}</strong>
                  <small>Capture all migrations by leaving dates blank, or narrow the worksheet before exporting and reviewing.</small>
                  {selectedCmsCoverageWarnings.length ? (
                    <small className="coverage-warning">
                      Removed-state export missing for {selectedCmsCoverageWarnings.join(", ")}. Treat CMS timing as partial.
                    </small>
                  ) : null}
                </div>
                <div className="migration-analysis-status">
                  <div className="analysis-stat">
                    <span>Selected leads</span>
                    <strong>{tray?.count ?? 0}</strong>
                  </div>
                  <div className="analysis-stat">
                    <span>SE eligible</span>
                    <strong>{seRankingSummary?.summary.eligibleCount ?? 0}</strong>
                  </div>
                  <div className="analysis-stat">
                    <span>To run</span>
                    <strong>{seRankingSummary?.summary.toRunCount ?? 0}</strong>
                  </div>
                </div>
              </div>

              <details className="source-coverage-panel accordion-section">
                <summary className="migration-analysis-section-header accordion-summary">
                  <div>
                    <span className="kicker-inline">BuiltWith source coverage</span>
                    <strong>Service CMS timing quality</strong>
                  </div>
                  <span className="accordion-indicator">Open</span>
                </summary>
                <div className="source-coverage-grid">
                  {(summary?.source_coverage ?? []).map((item) => (
                    <div className="source-coverage-row" key={item.platform}>
                      <div>
                        <strong>{humanizeToken(item.platform)}</strong>
                        <small>{item.notes.length ? item.notes.join(" · ") : "Current, recent, and removed exports present"}</small>
                      </div>
                      <div className="source-coverage-events">
                        <StatusBadge label={item.hasCurrent ? "Live" : "No live"} tone={item.hasCurrent ? "positive" : "neutral"} />
                        <StatusBadge label={item.hasRecent ? "Recent" : "No recent"} tone={item.hasRecent ? "positive" : "neutral"} />
                        <StatusBadge label={item.hasRemoved ? "Removed" : "No removed"} tone={item.hasRemoved ? "positive" : "warning"} />
                      </div>
                      <StatusBadge label={sourceCoverageLabel(item.timingQuality)} tone={sourceCoverageTone(item.timingQuality)} />
                    </div>
                  ))}
                </div>
              </details>

              <div className="migration-analysis-grid">
                <details className="migration-analysis-section accordion-section">
                  <summary className="migration-analysis-section-header accordion-summary">
                    <div>
                      <span className="kicker-inline">Worksheet migration filters</span>
                      <strong>Choose the migration dates you want visible in the sheet</strong>
                    </div>
                    <span className="accordion-indicator">Open</span>
                  </summary>

                  <div className="accordion-body">
                    <div className="migration-analysis-section-tools">
                      <div className="migration-timing-logic" role="group" aria-label="Migration timing logic">
                        <button
                          className={`ghost-button small-button ${query.migrationTimingOperator === "and" ? "timeline-toggle-active" : ""}`}
                          onClick={() => updateQuery({ migrationTimingOperator: "and" })}
                          type="button"
                        >
                          Match both
                        </button>
                        <button
                          className={`ghost-button small-button ${query.migrationTimingOperator === "or" ? "timeline-toggle-active" : ""}`}
                          onClick={() => updateQuery({ migrationTimingOperator: "or" })}
                          type="button"
                        >
                          Match either
                        </button>
                      </div>
                    </div>

                    <div className="migration-filter-grid">
                      <details className="migration-filter-card accordion-card">
                        <summary className="migration-filter-card-header accordion-summary">
                          <div>
                            <span className="kicker-inline">CMS migration</span>
                            <strong>{cmsTimingSummary}</strong>
                          </div>
                          <span className="accordion-indicator">Open</span>
                        </summary>
                        <div className="accordion-body">
                          <div className="migration-filter-fields">
                            <label className="field compact-field">
                              <span>From</span>
                              <input type="date" value={query.cmsMigrationFrom} onChange={(event) => updateQuery({ cmsMigrationFrom: event.target.value })} />
                            </label>
                            <label className="field compact-field">
                              <span>To</span>
                              <input type="date" value={query.cmsMigrationTo} onChange={(event) => updateQuery({ cmsMigrationTo: event.target.value })} />
                            </label>
                          </div>
                          <div className="migration-preset-row">
                            {MIGRATION_DATE_PRESETS.map((preset) => (
                              <button
                                key={`cms-${preset.label}`}
                                className="ghost-button small-button"
                                onClick={() => applyMigrationPreset("cms", preset.months)}
                                type="button"
                              >
                                {preset.label}
                              </button>
                            ))}
                            <button className="ghost-button small-button" onClick={clearCmsMigrationTiming} type="button">
                              All
                            </button>
                          </div>
                          <div className="migration-analysis-actions">
                            <button className="ghost-button small-button" disabled={!hasCmsTiming} onClick={clearCmsMigrationTiming} type="button">
                              Clear CMS dates
                            </button>
                          </div>
                        </div>
                      </details>

                      <details className="migration-filter-card accordion-card">
                        <summary className="migration-filter-card-header accordion-summary">
                          <div>
                            <span className="kicker-inline">Unchanged CMS</span>
                            <strong>{cmsUnchangedSummary}</strong>
                          </div>
                          <span className="accordion-indicator">Open</span>
                        </summary>
                        <div className="accordion-body">
                          <div className="migration-filter-fields">
                            <label className="field compact-field">
                              <span>Unchanged for at least</span>
                              <input
                                min="1"
                                max="50"
                                placeholder="Years"
                                type="number"
                                value={query.cmsUnchangedYears}
                                onChange={(event) => updateQuery({ cmsUnchangedYears: event.target.value })}
                              />
                            </label>
                          </div>
                          <p className="field-help">
                            Shows live leads whose current CMS was first detected before this window, with no newer CMS detected or CMS migration inside the same period.
                          </p>
                          <div className="migration-preset-row">
                            {[1, 2, 3, 5].map((years) => (
                              <button
                                key={`cms-unchanged-${years}`}
                                className="ghost-button small-button"
                                onClick={() => updateQuery({ cmsUnchangedYears: String(years) })}
                                type="button"
                              >
                                {years}y
                              </button>
                            ))}
                            <button className="ghost-button small-button" onClick={clearCmsUnchangedYears} type="button">
                              All
                            </button>
                          </div>
                          <div className="migration-analysis-actions">
                            <button className="ghost-button small-button" disabled={!hasCmsUnchangedYears} onClick={clearCmsUnchangedYears} type="button">
                              Clear unchanged filter
                            </button>
                          </div>
                        </div>
                      </details>

                      <details className="migration-filter-card accordion-card">
                        <summary className="migration-filter-card-header accordion-summary">
                          <div>
                            <span className="kicker-inline">Domain migration</span>
                            <strong>{domainTimingSummary}</strong>
                          </div>
                          <span className="accordion-indicator">Open</span>
                        </summary>
                        <div className="accordion-body">
                          <div className="migration-filter-fields">
                            <label className="field compact-field">
                              <span>From</span>
                              <input type="date" value={query.domainMigrationFrom} onChange={(event) => updateQuery({ domainMigrationFrom: event.target.value })} />
                            </label>
                            <label className="field compact-field">
                              <span>To</span>
                              <input type="date" value={query.domainMigrationTo} onChange={(event) => updateQuery({ domainMigrationTo: event.target.value })} />
                            </label>
                          </div>
                          <div className="migration-preset-row">
                            {MIGRATION_DATE_PRESETS.map((preset) => (
                              <button
                                key={`domain-${preset.label}`}
                                className="ghost-button small-button"
                                onClick={() => applyMigrationPreset("domain", preset.months)}
                                type="button"
                              >
                                {preset.label}
                              </button>
                            ))}
                            <button className="ghost-button small-button" onClick={clearDomainMigrationTiming} type="button">
                              All
                            </button>
                          </div>
                          <div className="migration-analysis-actions">
                            <button className="ghost-button small-button" disabled={!hasDomainTiming} onClick={clearDomainMigrationTiming} type="button">
                              Clear domain dates
                            </button>
                          </div>
                        </div>
                      </details>
                    </div>

                    <div className="migration-analysis-actions">
                      <button className="ghost-button small-button" disabled={!hasMigrationTiming} onClick={clearAllMigrationTiming} type="button">
                        Clear all migration filters
                      </button>
                    </div>
                  </div>
                </details>

                <details className="migration-analysis-section accordion-section">
                  <summary className="migration-analysis-section-header accordion-summary">
                    <div>
                      <span className="kicker-inline">Screaming Frog audits</span>
                      <strong>Run local technical crawls on selected leads</strong>
                    </div>
                    <div className="accordion-summary-meta">
                      <small>{screamingFrogLabel}</small>
                      <span className="accordion-indicator">Open</span>
                    </div>
                  </summary>

                  <div className="accordion-body">
                    <div className="se-analysis-grid">
                      <details className="se-analysis-card accordion-card">
                        <summary className="migration-filter-card-header accordion-summary">
                          <div>
                            <span className="kicker-inline">Local crawl</span>
                            <strong>Run Screaming Frog on this Mac and save the summary</strong>
                          </div>
                          <span className="accordion-indicator">Open</span>
                        </summary>
                        <div className="accordion-body">
                          <div className="se-ranking-controls">
                            <label className="field compact-field se-ranking-field">
                              <span>Crawl mode</span>
                              <select value={screamingFrogCrawlMode} onChange={(event) => setScreamingFrogCrawlMode(event.target.value as ScreamingFrogCrawlMode)}>
                                <option value="bounded_audit">Bounded audit</option>
                                <option value="deep_audit">Deep audit</option>
                              </select>
                            </label>
                            <div className="se-ranking-estimate">
                              <span>{screamingFrogLabel}</span>
                              <span>{screamingFrogSummary ? `${screamingFrogSummary.summary.estimatedRuns} local crawls` : "Preview to confirm run volume"}</span>
                              <span>Already audited {screamingFrogSummary?.summary.alreadyAuditedCount ?? 0}</span>
                              <span>Auto-selects the best local config for the detected CMS</span>
                              <span>{screamingFrogBreakdown || "Unknown or mixed sites use the generic fallback config"}</span>
                            </div>
                          </div>
                          {screamingFrogRunStatus ? (
                            <div className={`inline-run-status tone-${screamingFrogRunStatus.tone}`}>
                              <div className="inline-run-status-header">
                                <StatusBadge label={screamingFrogRunStatus.label} tone={screamingFrogRunStatus.tone} />
                                {screamingFrogLoading ? <strong>Running locally…</strong> : null}
                              </div>
                              <p>{screamingFrogRunStatus.message}</p>
                            </div>
                          ) : null}
                          <div className="se-ranking-actions">
                            <button
                              className="ghost-button small-button danger-button"
                              disabled={!tray?.count || screamingFrogLoading}
                              onClick={() => void clearTraySelection()}
                              type="button"
                            >
                              Clear selection
                            </button>
                            <button
                              className="ghost-button small-button"
                              disabled={!tray?.count || screamingFrogLoading}
                              onClick={() => void handleRunScreamingFrogAudit(false)}
                              type="button"
                            >
                              {screamingFrogLoading ? "Loading…" : "Preview run"}
                            </button>
                            <button
                              className="primary-button small-button"
                              disabled={!screamingFrogSummary?.summary.toRunCount || screamingFrogLoading}
                              onClick={() => void handleRunScreamingFrogAudit(true)}
                              type="button"
                            >
                              {screamingFrogLoading ? "Running…" : "Run audit"}
                            </button>
                            <button
                              className="ghost-button small-button danger-button"
                              disabled={!screamingFrogJobBatch?.isActive || screamingFrogLoading}
                              onClick={() => void handleStopScreamingFrogAudit()}
                              type="button"
                            >
                              Stop crawl
                            </button>
                            <button
                              className="ghost-button small-button"
                              disabled={!screamingFrogSummary?.summary.alreadyAuditedCount || screamingFrogLoading}
                              onClick={() => void handleRefreshScreamingFrogAudit()}
                              type="button"
                            >
                              Refresh audited results
                            </button>
                          </div>
                        </div>
                      </details>

                      <details className="se-analysis-card accordion-card">
                        <summary className="migration-filter-card-header accordion-summary">
                          <div>
                            <span className="kicker-inline">Audit output</span>
                            <strong>View the latest Screaming Frog results beside the controls</strong>
                          </div>
                          <span className="accordion-indicator">Open</span>
                        </summary>
                        <div className="accordion-body">
                          {screamingFrogJobBatch ? (
                            <div className="inline-audit-placeholder">
                              <strong>{screamingFrogJobBatch.isActive ? "Live crawl monitor" : "Latest crawl batch"}</strong>
                              <p>
                                Queued {screamingFrogJobCounts.queued ?? 0} · Discovering {screamingFrogJobCounts.discovering ?? 0} · Running {screamingFrogJobCounts.running ?? 0} · Success {screamingFrogJobCounts.success ?? 0} · Partial {screamingFrogJobCounts.partial ?? 0} · Error {screamingFrogJobCounts.error ?? 0}
                              </p>
                            </div>
                          ) : null}
                          {screamingFrogJobBatch?.items?.length ? (
                            <div className="inline-audit-stack">
                              <div className="inline-audit-list">
                              {screamingFrogJobBatch.items.slice(0, 8).map((item) => (
                                <div className="inline-audit-item" key={item.id}>
                                  <div className="inline-audit-item-header">
                                    <strong>{item.root_domain}</strong>
                                    <StatusBadge label={humanizeToken(item.status)} tone={confidenceTone(item.status)} />
                                  </div>
                                  <div className="inline-audit-item-meta">
                                    <span>{humanizeToken(item.resolved_platform_family || "generic")}</span>
                                    <span>{item.seed_strategy ? humanizeToken(item.seed_strategy) : "Seed strategy pending"}</span>
                                    <span>{item.seed_count ? `${item.seed_count} seed URLs` : "No seeds yet"}</span>
                                    <span>{item.result_quality ? humanizeToken(item.result_quality) : "Pending result quality"}</span>
                                  </div>
                                  {item.message ? <p className="subtle-copy">{item.message}</p> : null}
                                  <div className="drawer-actions">
                                    <button className="ghost-button small-button" onClick={() => openScreamingFrogAudit(item.root_domain)} type="button">
                                      Open full audit
                                    </button>
                                  </div>
                                </div>
                              ))}
                              </div>
                              <div className="inline-log-panel">
                                <div className="inline-log-header">
                                  <strong>Crawl log</strong>
                                  <small>{screamingFrogJobBatch.items.length} jobs in batch</small>
                                </div>
                                <div className="inline-log-list">
                                  {screamingFrogJobBatch.items.map((item) => (
                                    <div className="inline-log-entry" key={`${item.id}-log`}>
                                      <span className={`inline-log-dot status-${item.status}`}></span>
                                      <div className="inline-log-copy">
                                        <strong>{item.root_domain}</strong>
                                        <small>
                                          {humanizeToken(item.status)}
                                          {item.updated_at ? ` · ${formatDate(item.updated_at)}` : ""}
                                        </small>
                                        <p>
                                          {item.message || "Waiting in queue"}
                                          {item.seed_strategy ? ` · ${humanizeToken(item.seed_strategy)}` : ""}
                                          {item.seed_count ? ` · ${item.seed_count} seeds` : ""}
                                        </p>
                                      </div>
                                    </div>
                                  ))}
                                </div>
                              </div>
                            </div>
                          ) : screamingFrogRecentResults.length ? (
                            <div className="inline-audit-list">
                              {screamingFrogRecentResults.slice(0, 8).map((result: ScreamingFrogRunResponse["results"][number]) => (
                                <div className="inline-audit-item" key={`${result.root_domain}-${result.status}`}>
                                  <div className="inline-audit-item-header">
                                    <strong>{result.root_domain}</strong>
                                    <StatusBadge label={humanizeToken(result.status)} tone={confidenceTone(result.status)} />
                                  </div>
                                  <div className="inline-audit-item-meta">
                                    <span>{humanizeToken(result.resolved_platform_family || "generic")}</span>
                                    <span>{result.pages_crawled ? `${formatNumber(result.pages_crawled)} pages` : "No page count yet"}</span>
                                    <span>{result.homepage_status_category ? humanizeToken(result.homepage_status_category) : "No homepage status yet"}</span>
                                  </div>
                                  {result.error_message ? <p className="subtle-copy">{result.error_message}</p> : null}
                                  <div className="drawer-actions">
                                    <button className="ghost-button small-button" onClick={() => openScreamingFrogAudit(result.root_domain)} type="button">
                                      Open full audit
                                    </button>
                                  </div>
                                </div>
                              ))}
                            </div>
                          ) : screamingFrogSummary ? (
                            <div className="inline-audit-placeholder">
                              <strong>{screamingFrogSummary.summary.toRunCount} audits ready to run</strong>
                              <p>
                                Run the audit and the latest per-domain results will appear here immediately, without needing to open the drawer or hunt in the worksheet.
                              </p>
                              <div className="shell-pill-row">
                                {screamingFrogSummary.summary.resolvedConfigBreakdown.map((item) => (
                                  <span className="shell-pill" key={item.platformFamily}>
                                    {item.label}: {item.count}
                                  </span>
                                ))}
                              </div>
                            </div>
                          ) : (
                            <div className="inline-audit-placeholder">
                              <strong>No audit results yet</strong>
                              <p>Select tray leads and preview or run a Screaming Frog audit to populate this panel.</p>
                            </div>
                          )}
                        </div>
                      </details>
                    </div>
                  </div>
                </details>

                <details className="migration-analysis-section accordion-section">
                  <summary className="migration-analysis-section-header accordion-summary">
                    <div>
                      <span className="kicker-inline">Site status checks</span>
                      <strong>Check for 404s, redirects, and unreachable sites</strong>
                    </div>
                    <div className="accordion-summary-meta">
                      <small>{siteStatusLabel}</small>
                      <span className="accordion-indicator">Open</span>
                    </div>
                  </summary>

                  <div className="accordion-body">
                    <div className="se-analysis-grid">
                      <details className="se-analysis-card accordion-card">
                        <summary className="migration-filter-card-header accordion-summary">
                          <div>
                            <span className="kicker-inline">Bulk site check</span>
                            <strong>Run saved website health checks on selected leads</strong>
                          </div>
                          <span className="accordion-indicator">Open</span>
                        </summary>
                        <div className="accordion-body">
                          <div className="se-ranking-controls">
                            <div className="se-ranking-estimate">
                              <span>{siteStatusLabel}</span>
                              <span>{siteStatusSummary ? `${siteStatusSummary.summary.estimatedRequests} requests` : "Preview to confirm volume"}</span>
                              <span>Already checked {siteStatusSummary?.summary.alreadyCheckedCount ?? 0}</span>
                            </div>
                          </div>
                          <div className="se-ranking-actions">
                            <button
                              className="ghost-button small-button danger-button"
                              disabled={!tray?.count || siteStatusLoading}
                              onClick={() => void clearTraySelection()}
                              type="button"
                            >
                              Clear selection
                            </button>
                            <button
                              className="ghost-button small-button"
                              disabled={!tray?.count || siteStatusLoading}
                              onClick={() => void handleRunSiteStatusCheck(false)}
                              type="button"
                            >
                              {siteStatusLoading ? "Checking…" : "Preview run"}
                            </button>
                            <button
                              className="primary-button small-button"
                              disabled={!siteStatusSummary?.summary.toRunCount || siteStatusLoading}
                              onClick={() => void handleRunSiteStatusCheck(true)}
                              type="button"
                            >
                              {siteStatusLoading ? "Running…" : "Run site check"}
                            </button>
                            <button
                              className="ghost-button small-button"
                              disabled={!siteStatusSummary?.summary.alreadyCheckedCount || siteStatusLoading}
                              onClick={() => void handleRefreshSiteStatusCheck()}
                              type="button"
                            >
                              Refresh checked results
                            </button>
                          </div>
                        </div>
                      </details>
                    </div>
                  </div>
                </details>

                <details className="migration-analysis-section accordion-section">
                  <summary className="migration-analysis-section-header accordion-summary">
                    <div>
                      <span className="kicker-inline">SE Ranking checks</span>
                      <strong>Run SEO checks on selected migrations</strong>
                    </div>
                    <div className="accordion-summary-meta">
                      <small>{seEligibilityLabel}</small>
                      <span className="accordion-indicator">Open</span>
                    </div>
                  </summary>

                  <div className="accordion-body">
                    <div className="se-analysis-grid">
                      <details className="se-analysis-card accordion-card">
                        <summary className="migration-filter-card-header accordion-summary">
                          <div>
                            <span className="kicker-inline">Migration-based check</span>
                            <strong>One month before migration vs today</strong>
                          </div>
                          <span className="accordion-indicator">Open</span>
                        </summary>
                        <div className="accordion-body">
                          <div className="se-ranking-controls">
                            <label className="field compact-field se-ranking-field">
                              <span>Migration type</span>
                              <select value={seRankingType} onChange={(event) => setSeRankingType(event.target.value as SeRankingAnalysisType)}>
                                <option value="cms_migration">CMS migration</option>
                                <option value="domain_migration">Domain migration</option>
                              </select>
                            </label>
                            <div className="se-ranking-estimate">
                              <span>{seEligibilityLabel}</span>
                              <span>
                                {seRankingSummary
                                  ? `${seRankingSummary.summary.estimatedCredits} credits`
                                  : "Click preview run to confirm credits"}
                              </span>
                              <span>Already analyzed {seRankingSummary?.summary.alreadyAnalyzedCount ?? 0}</span>
                            </div>
                          </div>
                          <div className="se-ranking-actions">
                            <button className="ghost-button small-button" disabled={!leads?.total || seRankingLoading} onClick={() => void handleRunSeRankingAnalysis(false)} type="button">
                              {seRankingLoading ? "Checking…" : "Preview run"}
                            </button>
                            <button
                              className="primary-button small-button"
                              disabled={!seRankingSummary?.summary.toRunCount || seRankingLoading || seRankingSummaryDirty}
                              onClick={() => void handleRunSeRankingAnalysis(true)}
                              type="button"
                            >
                              {seRankingLoading ? "Running…" : "Run migration check"}
                            </button>
                            <button
                              className="ghost-button small-button"
                              disabled={!seRankingSummary?.summary.alreadyAnalyzedCount || seRankingLoading || seRankingSummaryDirty}
                              onClick={() => void handleRefreshSeRankingAnalysis()}
                              type="button"
                            >
                              Refresh displayed results
                            </button>
                          </div>
                          {seRankingSummaryDirty ? (
                            <p className="muted">Filters changed. Click <strong>Preview run</strong> to refresh the SE Ranking estimate for the current worksheet view.</p>
                          ) : null}
                        </div>
                      </details>

                      <details className="se-analysis-card accordion-card">
                        <summary className="migration-filter-card-header accordion-summary">
                          <div>
                            <span className="kicker-inline">Manual comparison</span>
                            <strong>Compare any two months for the selected leads</strong>
                          </div>
                          <span className="accordion-indicator">Open</span>
                        </summary>
                        <div className="accordion-body">
                          <div className="se-ranking-controls manual-se-ranking-controls">
                            <label className="field compact-field se-ranking-field">
                              <span>First month</span>
                              <input type="month" value={manualSeFirstMonth} onChange={(event) => setManualSeFirstMonth(event.target.value)} />
                            </label>
                            <label className="field compact-field se-ranking-field">
                              <span>Second month</span>
                              <input type="month" value={manualSeSecondMonth} onChange={(event) => setManualSeSecondMonth(event.target.value)} />
                            </label>
                            <div className="se-ranking-estimate">
                              <span>
                                {manualSePreview
                                  ? `${manualSePreview.summary.eligibleCount} eligible · ${manualSePreview.summary.estimatedCredits} credits`
                                  : "Choose two months to compare selected leads"}
                              </span>
                              <span>Preview {manualSePreview?.summary.estimatedRequests ?? 0} requests</span>
                              <span>Skipped {manualSePreview?.summary.excluded.length ?? 0}</span>
                            </div>
                          </div>
                          <div className="se-ranking-actions">
                            <button
                              className="ghost-button small-button danger-button"
                              disabled={!tray?.count || manualSeLoading}
                              onClick={() => void clearTraySelection()}
                              type="button"
                            >
                              Clear selection
                            </button>
                            <button
                              className="ghost-button small-button"
                              disabled={!tray?.count || !hasManualSeMonths || manualSeLoading}
                              onClick={() => void handlePreviewManualSeRankingAnalysis()}
                              type="button"
                            >
                              {manualSeLoading ? "Checking…" : "Preview run"}
                            </button>
                            <button
                              className="primary-button small-button"
                              disabled={!tray?.count || !hasManualSeMonths || manualSeLoading}
                              onClick={() => void handleRunManualSeRankingAnalysis()}
                              type="button"
                            >
                              {manualSeLoading ? "Running…" : "Run comparison"}
                            </button>
                          </div>
                        </div>
                      </details>
                    </div>
                  </div>
                </details>
              </div>
            </article>
          </section>

          {hasVisibleSeRankingData || query.hasSeRankingAnalysis ? (
            <div className="se-sort-row">
              <span className="se-sort-label">SE performance sort</span>
              <button
                className={`ghost-button small-button ${query.sortBy === "se_ranking_traffic_delta_percent" && query.sortDirection === "desc" ? "timeline-toggle-active" : ""}`}
                onClick={() => setExplicitSort("se_ranking_traffic_delta_percent", "desc")}
                type="button"
              >
                Traffic winners
              </button>
              <button
                className={`ghost-button small-button ${query.sortBy === "se_ranking_traffic_delta_percent" && query.sortDirection === "asc" ? "timeline-toggle-active" : ""}`}
                onClick={() => setExplicitSort("se_ranking_traffic_delta_percent", "asc")}
                type="button"
              >
                Traffic losers
              </button>
              <button
                className={`ghost-button small-button ${query.sortBy === "se_ranking_keywords_delta_percent" && query.sortDirection === "desc" ? "timeline-toggle-active" : ""}`}
                onClick={() => setExplicitSort("se_ranking_keywords_delta_percent", "desc")}
                type="button"
              >
                Keyword winners
              </button>
              <button
                className={`ghost-button small-button ${query.sortBy === "se_ranking_keywords_delta_percent" && query.sortDirection === "asc" ? "timeline-toggle-active" : ""}`}
                onClick={() => setExplicitSort("se_ranking_keywords_delta_percent", "asc")}
                type="button"
              >
                Keyword losers
              </button>
            </div>
          ) : null}

          {error && leadTableState !== "error" && leadTableState !== "retrying" ? <div className="error-box">{error}</div> : null}

          {leadStateMessage && (leadTableState !== "ready" || !leads?.items.length) ? (
            <div className={leadTableState === "error" || leadTableState === "retrying" ? "error-box" : "empty-state"}>
              {leadStateMessage}
            </div>
          ) : null}

          <div className="table-shell">
            <table className="sales-grid">
              <thead>
                <tr>
                  <th className="sticky sticky-select">
                    <input
                      checked={Boolean(leads?.items.length) && (leads?.items ?? []).every((item) => traySet.has(item.root_domain))}
                      onChange={toggleTrayForPage}
                      type="checkbox"
                    />
                  </th>
                  <SortableHeader label="Company" sortKey="company" query={query} onSort={setSort} stickyClass="sticky sticky-company" />
                  <SortableHeader label="Domain" sortKey="root_domain" query={query} onSort={setSort} stickyClass="sticky sticky-domain" />
                  {effectiveVisibleColumns.map((column) => (
                    <ColumnHeader column={column} key={column} query={query} onSort={setSort} />
                  ))}
                </tr>
              </thead>
              <tbody>
                {leads?.items.map((lead) => {
                  const selected = traySet.has(lead.root_domain);
                  return (
                    <tr
                      className={selectedLeadId === lead.root_domain ? "row-active" : ""}
                      key={lead.root_domain}
                      onClick={() => setSelectedLeadId(lead.root_domain)}
                    >
                      <td className="sticky sticky-select" onClick={(event) => event.stopPropagation()}>
                        <input checked={selected} onChange={() => void toggleTrayForLead(lead)} type="checkbox" />
                      </td>
                      <td className="sticky sticky-company company-cell">
                        <strong>{lead.company || "Unknown company"}</strong>
                      </td>
                      <td className="sticky sticky-domain mono-cell">
                        <div className="domain-cell">
                          <span>{lead.root_domain}</span>
                          <a
                            className="domain-open-link"
                            href={`https://${lead.root_domain}`}
                            onClick={(event) => event.stopPropagation()}
                            rel="noopener noreferrer"
                            target="_blank"
                            title={`Open ${lead.root_domain}`}
                          >
                            ↗
                          </a>
                        </div>
                      </td>
                      {effectiveVisibleColumns.map((column) => (
                        <td key={column}>{renderCell(column, lead, query.salesBuckets, openScreamingFrogAudit)}</td>
                      ))}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>

          <div className="pagination-bar">
            <button disabled={(leads?.page ?? 1) <= 1} onClick={() => updateQuery({ page: (leads?.page ?? 1) - 1 }, true)} type="button">
              Previous
            </button>
            <button
              disabled={(leads?.page ?? 1) >= (leads?.pages ?? 1)}
              onClick={() => updateQuery({ page: (leads?.page ?? 1) + 1 }, true)}
              type="button"
            >
              Next
            </button>
          </div>
        </section>
      </main>

      <aside className={`detail-drawer ${selectedLeadId ? "drawer-open" : ""}`}>
        <div className="drawer-header">
          <div>
            <p className="kicker">Inspector</p>
            <h2>{detail?.lead.company || selectedLeadId || "No lead selected"}</h2>
          </div>
          <button className="ghost-button small-button" onClick={() => setSelectedLeadId(null)} type="button">
            Close
          </button>
        </div>

        {!selectedLeadId ? (
          <div className="drawer-placeholder">Click a row to inspect the lead, stack, and export readiness.</div>
        ) : detailLoading || !detail ? (
          <div className="drawer-placeholder">Loading lead detail…</div>
        ) : (
          <>
            <section className="drawer-section">
              <div className="drawer-hero">
                <div className="drawer-hero-top">
                  <div>
                    <p className="drawer-domain">{detail.lead.root_domain}</p>
                    <div className="drawer-meta">
                      <span>{humanizeToken(detail.lead.country)}</span>
                      <span>Tier {detail.lead.priority_tier}</span>
                      <span>Score {detail.lead.total_score}</span>
                      {detail.lead.vertical ? <span>{detail.lead.vertical}</span> : null}
                    </div>
                  </div>
                  <div className="drawer-nav">
                    <button onClick={() => navigateDrawer("prev")} type="button">
                      Previous
                    </button>
                    <button onClick={() => navigateDrawer("next")} type="button">
                      Next
                    </button>
                  </div>
                </div>
                <div className="evidence-strip">
                  <StatusBadge
                    label={evidenceQualityLabel(detail.lead)}
                    tone={confidenceTone(detail.lead.domain_migration_status || detail.lead.cms_migration_status)}
                  />
                  {detail.lead.domain_migration_status !== "none" ? (
                    <StatusBadge
                      label={`Previous domain: ${humanizeToken(detail.lead.domain_migration_status)}`}
                      tone={confidenceTone(detail.lead.domain_migration_status)}
                    />
                  ) : null}
                  {detail.lead.cms_migration_status !== "none" ? (
                    <StatusBadge
                      label={`CMS: ${humanizeToken(detail.lead.cms_migration_status)}`}
                      tone={confidenceTone(detail.lead.cms_migration_status)}
                    />
                  ) : null}
                </div>
                <div className="drawer-summary-grid">
                  <MiniMetric
                    label="Contacts"
                    value={`${detail.lead.emails.length + detail.lead.telephones.length + detail.lead.people.length}`}
                  />
                  <MiniMetric
                    label="Current stack"
                    value={
                      detail.lead.current_platforms[0]
                        ? humanizeToken(detail.lead.current_platforms[0])
                        : detail.lead.current_candidate_platforms[0]
                          ? humanizeToken(detail.lead.current_candidate_platforms[0])
                          : "Unknown"
                    }
                  />
                  <MiniMetric label="Lead angles" value={`${detail.lead.sales_buckets.length}`} />
                  <MiniMetric
                    label="SEO check"
                    value={detail.seRankingAnalysis ? humanizeToken(String(detail.seRankingAnalysis.status || "saved")) : "Not run"}
                    tone={detail.seRankingAnalysis ? confidenceTone(String(detail.seRankingAnalysis.status || "")) : "neutral"}
                  />
                  <MiniMetric
                    label="Site check"
                    value={detail.siteStatusCheck ? humanizeToken(String(detail.siteStatusCheck.status_category || "saved")) : "Not run"}
                    tone={detail.siteStatusCheck ? confidenceTone(String(detail.siteStatusCheck.status_category || "")) : "neutral"}
                  />
                </div>
              </div>
              {detail.data_quality.notes.length ? (
                <div className="pill-row">
                  {detail.data_quality.notes.slice(0, 4).map((note) => (
                    <span className="pill signal-pill" key={note}>
                      {note}
                    </span>
                  ))}
                </div>
              ) : null}
            </section>

            <section className="drawer-section">
              <h3>Migration summary</h3>
              <div className="migration-intelligence-grid">
                <MigrationHeadlineCard
                  title="Previous domain"
                  emptyText="No previous-domain match found yet."
                  populated={Boolean(detail.migrationIntelligence.domainMigration.bestMatch)}
                >
                  {detail.migrationIntelligence.domainMigration.bestMatch ? (
                    <DomainMigrationHeadline
                      currentDomain={detail.lead.root_domain}
                      bestMatch={detail.migrationIntelligence.domainMigration.bestMatch}
                    />
                  ) : null}
                </MigrationHeadlineCard>
                <MigrationHeadlineCard
                  title="CMS migration"
                  emptyText="No CMS migration matched yet."
                  populated={Boolean(detail.migrationIntelligence.cmsMigration.bestPair)}
                >
                  {detail.migrationIntelligence.cmsMigration.bestPair ? (
                    <CmsMigrationHeadline pair={detail.migrationIntelligence.cmsMigration.bestPair} />
                  ) : null}
                </MigrationHeadlineCard>
              </div>
              <div className="drawer-accordion-stack">
                <DrawerAccordion
                  title="Previous domain evidence"
                  subtitle={`${detail.migrationIntelligence.summary.domainCandidateCount} candidates`}
                  defaultOpen={Boolean(detail.migrationIntelligence.domainMigration.bestMatch)}
                >
                  {detail.migrationIntelligence.domainMigration.bestMatch ? (
                    <>
                      <SignalChipRow signals={detail.migrationIntelligence.domainMigration.bestMatch.shared_signal_flags ?? []} />
                      <TechnologyChipRow
                        label="Shared tech"
                        values={detail.migrationIntelligence.domainMigration.bestMatch.shared_high_signal_technologies ?? []}
                      />
                      <div className="comparison-strip">
                        <ComparisonTile label="Current company" value={detail.lead.company || "Unknown"} />
                        <ComparisonTile
                          label="Old company"
                          value={detail.migrationIntelligence.domainMigration.bestMatch.old_company || "Unknown"}
                        />
                        <ComparisonTile label="Current country" value={humanizeToken(detail.lead.country)} />
                        <ComparisonTile
                          label="Old country"
                          value={humanizeToken(detail.migrationIntelligence.domainMigration.bestMatch.old_country || "Unknown")}
                        />
                      </div>
                      <p className="migration-copy">{detail.migrationIntelligence.domainMigration.bestMatch.notes || "No summary note available."}</p>
                      {detail.migrationIntelligence.domainMigration.bestMatch.fingerprint_notes ? (
                        <p className="migration-copy subtle-copy">{detail.migrationIntelligence.domainMigration.bestMatch.fingerprint_notes}</p>
                      ) : null}
                      <DomainCandidateList candidates={detail.migrationIntelligence.domainMigration.candidateShortlist} />
                    </>
                  ) : (
                    <p className="muted">No previous-domain evidence available for this lead.</p>
                  )}
                </DrawerAccordion>

                <DrawerAccordion
                  title="CMS migration evidence"
                  subtitle={`${detail.migrationIntelligence.summary.cmsCandidateCount} candidates`}
                  defaultOpen={Boolean(detail.migrationIntelligence.cmsMigration.candidatePairs.length)}
                >
                  {detail.migrationIntelligence.cmsMigration.candidatePairs.length ? (
                    <>
                      <SignalChipRow signals={detail.lead.cms_migration_warning_flags} emptyText="No CMS migration warnings for this lead." />
                      <TechnologyChipRow label="Evidence" values={detail.lead.cms_migration_evidence_flags} />
                      <MigrationList migrations={detail.migrationIntelligence.cmsMigration.candidatePairs.slice(0, 4)} />
                    </>
                  ) : (
                    <p className="muted">No CMS migration evidence available for this lead.</p>
                  )}
                </DrawerAccordion>

                <DrawerAccordion title="Timeline and raw events" subtitle={`${detail.timelineRows.length} timelines · ${detail.events.length} events`}>
                  <DrawerTimelineChart rows={detail.timelineRows} />
                  <ul className="event-list compact-list">
                    {detail.events.slice(0, 8).map((event, index) => (
                      <li key={`${event.platform}-${event.event_type}-${index}`}>
                        <strong>
                          {humanizeToken(event.platform)} · {humanizeToken(event.event_type)}
                        </strong>
                        <span>
                          first seen {event.first_detected || "n/a"} · last seen {event.last_found || "n/a"}
                        </span>
                      </li>
                    ))}
                  </ul>
                </DrawerAccordion>
              </div>
            </section>

            <section className="drawer-section">
              <h3>Prospecting snapshot</h3>
              <div className="contact-preview-grid">
                <ContactPreview title="Emails" items={detail.lead.emails} />
                <ContactPreview title="Phones" items={detail.lead.telephones} />
                <ContactPreview title="People" items={detail.lead.people} />
                <ContactPreview title="Verified" items={detail.lead.verified_profiles} />
              </div>
              <div className="drawer-accordion-stack">
                <DrawerAccordion title="Stack details" subtitle="Current and previous platforms, plus key tooling">
                  <DrawerPillGroup title="Current platform" items={detail.lead.current_platforms.length ? detail.lead.current_platforms : detail.lead.current_candidate_platforms} />
                  <DrawerPillGroup title="Previous platform seen" items={detail.lead.removed_platforms} />
                  <DrawerPillGroup title="Marketing" items={detail.lead.marketing_platforms} />
                  <DrawerPillGroup title="CRM" items={detail.lead.crm_platforms} />
                  <DrawerPillGroup title="Payments" items={detail.lead.payment_platforms} />
                  <DrawerPillGroup title="Hosting" items={detail.lead.hosting_providers} />
                </DrawerAccordion>
                <DrawerAccordion title="Bucket evidence" subtitle={`${detail.exportReady.bucket_reasons.length} reasons`}>
                  <div className="pill-row">{pillList(detail.lead.sales_buckets, 10)}</div>
                  <ul className="reason-list">
                    {detail.exportReady.bucket_reasons.map((reason) => (
                      <li key={reason}>{humanizeReason(reason)}</li>
                    ))}
                  </ul>
                </DrawerAccordion>
              </div>
            </section>

            <section className="drawer-section">
              <h3>Site status</h3>
              {detail.siteStatusCheck ? (
                <DrawerAccordion
                  title="Saved site check"
                  subtitle={`Checked ${formatDate(String(detail.siteStatusCheck.checked_at || ""))}`}
                  defaultOpen
                >
                  <div className="se-drawer-grid">
                    <ComparisonTile label="Status" value={humanizeToken(String(detail.siteStatusCheck.status_category || "unknown"))} />
                    <ComparisonTile label="Status code" value={String(detail.siteStatusCheck.status_code || "—")} />
                    <ComparisonTile label="Final URL" value={String(detail.siteStatusCheck.final_url || "—")} />
                    <ComparisonTile label="Redirect count" value={String(detail.siteStatusCheck.redirect_count || 0)} />
                  </div>
                  {detail.siteStatusCheck.error_message ? <p className="migration-copy">{String(detail.siteStatusCheck.error_message)}</p> : null}
                </DrawerAccordion>
              ) : (
                <p className="muted">No site status check has been saved for this lead.</p>
              )}
            </section>

            <section className="drawer-section">
              <h3>SE Ranking outcome</h3>
              {detail.seRankingAnalysis ? (
                <DrawerAccordion
                  title="Saved SEO outcome"
                  subtitle={`${String(detail.seRankingAnalysis.regional_source || "—").toUpperCase()} market · checked ${formatDate(String(detail.seRankingAnalysis.captured_at || ""))}`}
                  defaultOpen
                >
                  <div className="se-drawer-grid">
                    <ComparisonTile label="Comparison mode" value={humanizeToken(String(detail.seRankingAnalysis.analysis_mode || detail.seRankingAnalysis.analysis_type || "unknown"))} />
                    <ComparisonTile
                      label={String(detail.seRankingAnalysis.analysis_mode || "") === "manual" ? "First month" : "Migration date"}
                      value={
                        String(detail.seRankingAnalysis.analysis_mode || "") === "manual"
                          ? formatMonthYear(String(detail.seRankingAnalysis.date_label_first || detail.seRankingAnalysis.first_comparison_month || ""))
                          : formatDate(String(detail.seRankingAnalysis.migration_likely_date || ""))
                      }
                    />
                    <ComparisonTile
                      label={String(detail.seRankingAnalysis.analysis_mode || "") === "manual" ? "Second month" : "Comparison month"}
                      value={
                        String(detail.seRankingAnalysis.analysis_mode || "") === "manual"
                          ? formatMonthYear(String(detail.seRankingAnalysis.date_label_second || detail.seRankingAnalysis.second_comparison_month || ""))
                          : formatMonthYear(String(detail.seRankingAnalysis.comparison_month || detail.seRankingAnalysis.date_label_second || ""))
                      }
                    />
                    <ComparisonTile label="Traffic first month" value={formatNumber(Number(detail.seRankingAnalysis.traffic_before || 0))} />
                    <ComparisonTile label="Traffic second month" value={formatNumber(Number(detail.seRankingAnalysis.traffic_last_month || 0))} />
                    <ComparisonTile label="Keywords first month" value={formatNumber(Number(detail.seRankingAnalysis.keywords_before || 0))} />
                    <ComparisonTile label="Keywords second month" value={formatNumber(Number(detail.seRankingAnalysis.keywords_last_month || 0))} />
                  </div>
                  <div className="pill-row">
                    {(detail.seRankingAnalysis.outcome_flags ?? []).map((flag) => (
                      <span className="pill signal-pill" key={flag}>
                        {humanizeToken(flag)}
                      </span>
                    ))}
                  </div>
                  <p className="migration-copy">
                    Traffic change <strong>{formatPercent(detail.lead.se_ranking_traffic_delta_percent)}</strong> ({formatSignedNumber(detail.lead.se_ranking_traffic_delta_absolute)}) · keyword change <strong>{formatPercent(detail.lead.se_ranking_keywords_delta_percent)}</strong> ({formatSignedNumber(detail.lead.se_ranking_keywords_delta_absolute)})
                  </p>
                  {detail.lead.se_ranking_error_message ? <p className="migration-copy subtle-copy">{detail.lead.se_ranking_error_message}</p> : null}
                </DrawerAccordion>
              ) : (
                <p className="muted">No SE Ranking analysis saved for this lead yet.</p>
              )}
            </section>

            <section className="drawer-section">
              <h3>Screaming Frog audit</h3>
              {detail.screamingFrogAudit ? (
                <DrawerAccordion
                  title="Saved local crawl"
                  subtitle={`${humanizeToken(String(detail.screamingFrogAudit.crawl_mode || "bounded_audit"))} · ${humanizeToken(String(detail.screamingFrogAudit.resolved_platform_family || "generic"))} · checked ${formatDate(String(detail.screamingFrogAudit.checked_at || ""))}`}
                  defaultOpen
                >
                  <div className="se-drawer-grid">
                    <ComparisonTile label="Audit status" value={humanizeToken(String(detail.screamingFrogAudit.status || "unknown"))} />
                    <ComparisonTile label="Resolved config" value={humanizeToken(String(detail.screamingFrogAudit.resolved_platform_family || "generic"))} />
                    <ComparisonTile label="Result quality" value={humanizeToken(String(detail.screamingFrogAudit.result_quality || "unknown"))} />
                    <ComparisonTile label="Seed strategy" value={humanizeToken(String(detail.screamingFrogAudit.seed_strategy || "unknown"))} />
                    <ComparisonTile label="Pages crawled" value={String(detail.screamingFrogAudit.pages_crawled || 0)} />
                    <ComparisonTile label="Homepage status" value={humanizeToken(String(detail.screamingFrogAudit.homepage_status_category || "unknown"))} />
                    <ComparisonTile label="Homepage code" value={String(detail.screamingFrogAudit.homepage_status_code || "—")} />
                    <ComparisonTile label="Opportunity score" value={String(detail.screamingFrogAudit.sf_opportunity_score || 0)} />
                    <ComparisonTile label="Primary issue" value={humanizeToken(String(detail.screamingFrogAudit.sf_primary_issue_family || "none"))} />
                    <ComparisonTile label="Title issues" value={splitPipe(String(detail.screamingFrogAudit.title_issue_flags || "")).map(humanizeToken).join(", ") || "None"} />
                    <ComparisonTile label="Meta issues" value={splitPipe(String(detail.screamingFrogAudit.meta_issue_flags || "")).map(humanizeToken).join(", ") || "None"} />
                    <ComparisonTile label="Canonical issues" value={splitPipe(String(detail.screamingFrogAudit.canonical_issue_flags || "")).map(humanizeToken).join(", ") || "None"} />
                    <ComparisonTile label="Internal errors" value={`${detail.screamingFrogAudit.internal_4xx_count || 0} 4xx · ${detail.screamingFrogAudit.internal_5xx_count || 0} 5xx`} />
                    <ComparisonTile label="Category pages" value={String(detail.screamingFrogAudit.category_page_count || 0)} />
                    <ComparisonTile label="Product pages" value={String(detail.screamingFrogAudit.product_page_count || 0)} />
                    <ComparisonTile label="Location pages" value={String(detail.screamingFrogAudit.location_page_count || 0)} />
                    <ComparisonTile label="Service pages" value={String(detail.screamingFrogAudit.service_page_count || 0)} />
                    <ComparisonTile label="Collection detection" value={`${humanizeToken(String(detail.screamingFrogAudit.collection_detection_status || "unknown"))} · ${detail.screamingFrogAudit.collection_detection_confidence || 0}%`} />
                    <ComparisonTile label="Collection intro" value={humanizeToken(String(detail.screamingFrogAudit.collection_intro_status || "unknown"))} />
                    <ComparisonTile label="Title optimisation" value={humanizeToken(String(detail.screamingFrogAudit.title_optimization_status || "unknown"))} />
                    <ComparisonTile label="Collection products" value={String(detail.screamingFrogAudit.collection_product_count || 0)} />
                    <ComparisonTile label="Collection schema" value={splitPipe(String(detail.screamingFrogAudit.collection_schema_types || "")).map(humanizeToken).join(", ") || "None"} />
                  </div>
                  {String(detail.screamingFrogAudit.sf_primary_issue_reason || "") ? (
                    <p className="migration-copy subtle-copy">{String(detail.screamingFrogAudit.sf_primary_issue_reason || "")}</p>
                  ) : null}
                  {String(detail.screamingFrogAudit.collection_issue_reason || "") ? (
                    <p className="migration-copy subtle-copy">Collection read: {String(detail.screamingFrogAudit.collection_issue_reason || "")}</p>
                  ) : null}
                  {String(detail.screamingFrogAudit.collection_intro_text || "") ? (
                    <p className="migration-copy subtle-copy">Collection intro snippet: {String(detail.screamingFrogAudit.collection_intro_text || "")}</p>
                  ) : null}
                  {Array.isArray(detail.lead.screamingfrog_outreach_hooks) && detail.lead.screamingfrog_outreach_hooks.length ? (
                    <p className="migration-copy subtle-copy">Email hooks: {detail.lead.screamingfrog_outreach_hooks.join(" | ")}</p>
                  ) : null}
                  <div className="drawer-actions">
                    <button className="ghost-button small-button" onClick={() => openScreamingFrogAudit(detail.lead.root_domain)} type="button">
                      Open full audit
                    </button>
                    {detail.screamingFrogAudit.export_directory ? (
                      <button className="ghost-button small-button" onClick={() => void copyToClipboard(String(detail.screamingFrogAudit?.export_directory || ""), "audit folder path")} type="button">
                        Copy audit path
                      </button>
                    ) : null}
                  </div>
                  {detail.screamingFrogAudit.export_directory ? <p className="migration-copy subtle-copy">Saved exports: {String(detail.screamingFrogAudit.export_directory)}</p> : null}
                  {detail.screamingFrogAudit.error_message ? <p className="migration-copy">{String(detail.screamingFrogAudit.error_message)}</p> : null}
                </DrawerAccordion>
              ) : (
                <p className="muted">No Screaming Frog audit has been saved for this lead.</p>
              )}
            </section>

            <section className="drawer-section">
              <h3>Export actions</h3>
              <div className="drawer-actions">
                <button className="primary-button" onClick={() => void toggleTrayForLead(detail.lead)} type="button">
                  {traySet.has(detail.lead.root_domain) ? "Remove from tray" : "Add to tray"}
                </button>
                <button className="ghost-button" onClick={() => void copyToClipboard(detail.lead.root_domain, "domain")} type="button">
                  Copy domain
                </button>
                <button className="ghost-button" onClick={() => void copyToClipboard(detail.lead.emails.join("\n"), "emails")} type="button">
                  Copy emails
                </button>
                <button className="ghost-button" onClick={() => exportSingleLead(detail)} type="button">
                  Export row CSV
                </button>
              </div>
            </section>
          </>
        )}
      </aside>

      <section className={`export-tray ${tray?.count ? "tray-open" : ""} ${effectiveTrayCollapsed ? "tray-compact" : ""}`}>
        <div>
          <p className="kicker">Selection tray</p>
          <h2>{tray?.count ?? 0} selected leads</h2>
          {!effectiveTrayCollapsed ? (
            <div className="tray-mix">
              {(tray?.countryMix ?? []).map((item) => (
                <span className="tray-chip" key={item.label}>
                  {humanizeToken(item.label)} {item.count}
                </span>
              ))}
            </div>
          ) : null}
        </div>
        {!effectiveTrayCollapsed ? (
          <div className="tray-preview">
            {(tray?.items ?? []).slice(0, 4).map((item) => (
              <span className="tray-chip muted-chip" key={item.root_domain}>
                {item.company || item.root_domain}
              </span>
            ))}
          </div>
        ) : (
          <div className="tray-preview tray-preview-compact">
            <span className="tray-chip muted-chip">{tray?.items?.[0]?.company || tray?.items?.[0]?.root_domain || "Tray ready for export"}</span>
          </div>
        )}
        <div className="tray-actions">
          <button className="primary-button" disabled={!leads?.total || selectionLoading} onClick={() => void selectAllFilteredLeads()} type="button">
            {selectionLoading ? "Selecting…" : "Select all filtered"}
          </button>
          <button className="ghost-button" onClick={toggleTrayVisibility} type="button">
            {effectiveTrayCollapsed ? "Expand tray" : "Collapse tray"}
          </button>
          <button className="ghost-button" onClick={() => updateQuery({ selectedOnly: !query.selectedOnly })} type="button">
            {query.selectedOnly ? "Show all leads" : "View selected only"}
          </button>
          <a className="primary-button" href={exportLeadUrl({ ...query, selectedOnly: true, page: 1 })}>
            Export selected CSV
          </a>
          <button className="ghost-button" onClick={() => void clearTraySelection()} type="button">
            Clear tray
          </button>
        </div>
      </section>

      {presetModal ? (
        <div className="modal-backdrop" role="presentation">
          <div className="modal-card" role="dialog" aria-modal="true" aria-label="Preset modal">
            {presetModal.mode === "save" ? (
              <>
                <h3>Save preset</h3>
                <p className="muted">Store the current filters, sorting, and visible columns as a reusable view.</p>
                <label className="field compact-field">
                  <span>Preset name</span>
                  <input value={presetNameDraft} onChange={(event) => setPresetNameDraft(event.target.value)} placeholder="Migration review queue" />
                </label>
                <div className="modal-actions">
                  <button className="ghost-button" onClick={() => setPresetModal(null)} type="button">
                    Cancel
                  </button>
                  <button className="primary-button" onClick={() => void confirmSavePreset()} type="button">
                    Save
                  </button>
                </div>
              </>
            ) : (
              <>
                <h3>Delete preset</h3>
                <p className="muted">This removes the saved preset but leaves the current view untouched.</p>
                <div className="modal-actions">
                  <button className="ghost-button" onClick={() => setPresetModal(null)} type="button">
                    Cancel
                  </button>
                  <button className="primary-button" onClick={() => void confirmDeletePreset()} type="button">
                    Delete
                  </button>
                </div>
              </>
            )}
          </div>
        </div>
      ) : null}

      {showGuideModal ? (
        <div className="modal-backdrop" role="presentation">
          <div className="modal-card guide-modal" role="dialog" aria-modal="true" aria-label="Lead Console guide">
            <div className="guide-header">
              <div>
                <p className="kicker-inline">In-app README</p>
                <h3>How Lead Console works</h3>
              </div>
              <button className="ghost-button small-button" onClick={() => setShowGuideModal(false)} type="button">
                Close
              </button>
            </div>

            <div className="guide-body">
              <section className="guide-section">
                <h4>What this tool is for</h4>
                <p>
                  Lead Console is a desktop-first lead console for finding outreach angles from platform changes, domain changes, timing signals, and SEO outcomes.
                  It is designed for shortlist building, not CRM management.
                </p>
              </section>

              <section className="guide-section">
                <h4>Core workflow</h4>
                <ol className="guide-list guide-list-numbered">
                  <li>Start with a preset or a broad filter like country, vertical, current platform, or migration status.</li>
                  <li>Refine in the left rail using common filters first, then advanced tech or agency filters.</li>
                  <li>Review the spreadsheet for migration signals, timing, contact readiness, and SE Ranking outcome.</li>
                  <li>Open the right drawer for the best explanation of previous domain, CMS migration, stack, contacts, and evidence.</li>
                  <li>Add good leads to the selection tray, then export either the filtered set or the selected tray.</li>
                </ol>
              </section>

              <section className="guide-section">
                <h4>How filters work</h4>
                <ul className="guide-list">
                  <li>Filters inside the same group use OR. Example: two agencies means either agency can match.</li>
                  <li>Different filter groups combine with AND. Example: Australia + Shopify + a specific agency means all three must match.</li>
                  <li>Advanced tech and agency lists are scoped to the current worksheet filters, so they reflect the current result set.</li>
                  <li>`Select all filtered` adds the full filtered result set to the tray, not just the current page.</li>
                </ul>
              </section>

              <section className="guide-section">
                <h4>Migration signals</h4>
                <ul className="guide-list">
                  <li>`Previous domain candidate` is the best current previous-domain match from redirects and fingerprint evidence.</li>
                  <li>`Possible CMS migration` is the best current old-platform to new-platform interpretation.</li>
                  <li>`Confirmed`, `Possible`, `Overlap`, and similar labels are evidence states, not guarantees.</li>
                  <li>Same-TLD previous domains are often more useful for small owner-operated businesses, but cross-TLD relationships are still shown.</li>
                </ul>
              </section>

              <section className="guide-section">
                <h4>Migration controls and platform signals</h4>
                <ul className="guide-list">
                  <li>`First seen` and `Last seen` use BuiltWith timeline rows, not your own crawl dates.</li>
                  <li>The cohort chart only appears when one or more timeline technologies are selected.</li>
                  <li>Platform signal filters affect both the worksheet and exports.</li>
                  <li>The drawer timeline is for explanation and review; the worksheet remains the main selection surface.</li>
                </ul>
              </section>

              <section className="guide-section">
                <h4>SE Ranking analysis</h4>
                <ul className="guide-list">
                  <li>There are two SE workflows: migration-based analysis and manual month-to-month comparison.</li>
                  <li>Migration-based SE checks use the likely migration date and compare before vs last month.</li>
                  <li>You can leave migration date filters blank to review all migrations, then run SE checks only on selected migrations that are within the last 11 months.</li>
                  <li>Manual comparison lets you choose two months for the currently selected leads.</li>
                  <li>SE Ranking uses the lead country to choose the market automatically: AU, NZ, or SG.</li>
                  <li>The worksheet shows the latest saved SE result for a domain, whether it came from migration mode or manual mode.</li>
                </ul>
              </section>

              <section className="guide-section">
                <h4>Exports and selection tray</h4>
                <ul className="guide-list">
                  <li>`Export filtered CSV` exports the current worksheet result set.</li>
                  <li>`Export selected CSV` exports only domains in the tray.</li>
                  <li>The tray persists across paging and filter changes.</li>
                  <li>Use the tray when you want to collect leads from different filter combinations before exporting.</li>
                </ul>
              </section>

              <section className="guide-section">
                <h4>Important limitations</h4>
                <ul className="guide-list">
                  <li>BuiltWith data is probabilistic. Removal rows and snapshot platform fields can be noisy or conflicting.</li>
                  <li>A detected agency means BuiltWith associated that agency with the site; it does not prove a current formal relationship.</li>
                  <li>Migration signals are evidence-based interpretations, not legal or forensic proof.</li>
                  <li>SE Ranking history is monthly, so manual SEO comparison works at month level, not exact day level.</li>
                  <li>The app is scoped to AU, NZ, and SG only.</li>
                </ul>
              </section>

              <section className="guide-section">
                <h4>Best practice</h4>
                <p>
                  Start broad, confirm the story in the drawer, then export only the leads where the migration or platform story creates a clear outreach angle.
                  Use SE Ranking after you already have a shortlist, because it consumes credits.
                </p>
              </section>
            </div>
          </div>
        </div>
      ) : null}

      {toast ? <div className="toast">{toast}</div> : null}
    </div>
  );
}

const SidebarSection = memo(function SidebarSection(props: {
  title: string;
  description?: string;
  activeCount: number;
  open: boolean;
  onToggle: () => void;
  children: ReactNode;
}) {
  return (
    <section className="sidebar-section">
      <button className="sidebar-section-header" onClick={props.onToggle} type="button">
        <span>{props.title}</span>
        <div>
          <small>{props.activeCount} active</small>
          <strong>{props.open ? "−" : "+"}</strong>
        </div>
      </button>
      {props.open ? (
        <div className="sidebar-section-body">
          {props.description ? <p className="sidebar-section-description">{props.description}</p> : null}
          {props.children}
        </div>
      ) : null}
    </section>
  );
});

const FilterBlock = memo(function FilterBlock(props: {
  title: string;
  items: string[];
  selected: string[];
  onToggle: (value: string) => void;
  onSelectAll?: () => void;
  onClearAll?: () => void;
  formatLabel?: (value: string) => string;
  searchable?: boolean;
}) {
  const [open, setOpen] = useState(props.selected.length > 0 || props.items.length <= 8);
  const [search, setSearch] = useState("");
  const selectedSet = useMemo(() => new Set(props.selected), [props.selected]);

  useEffect(() => {
    if (props.selected.length > 0) {
      setOpen(true);
    }
  }, [props.selected.length]);

  const deferredSearch = useDeferredValue(search);
  const normalizedSearch = deferredSearch.trim().toLowerCase();
  const filteredItems = useMemo(() => {
    if (!normalizedSearch) {
      return props.items;
    }
    return props.items.filter((item) => (props.formatLabel ? props.formatLabel(item) : item).toLowerCase().includes(normalizedSearch));
  }, [normalizedSearch, props.formatLabel, props.items]);
  const showInlineSearch = Boolean(props.searchable && props.items.length > FILTER_SEARCH_THRESHOLD);

  return (
    <section className="filter-block filter-block-collapsible">
      <div className="filter-block-header">
        <button className="filter-block-toggle" onClick={() => setOpen((current) => !current)} type="button">
          <span>{props.title}</span>
          <small>{props.selected.length ? `${props.selected.length} selected` : `${props.items.length} options`}</small>
        </button>
        <div className="filter-block-actions">
          {props.onSelectAll ? (
            <button className="filter-block-action" disabled={!props.items.length || props.selected.length === props.items.length} onClick={props.onSelectAll} type="button">
              Select all
            </button>
          ) : null}
          {props.onClearAll ? (
            <button className="filter-block-action" disabled={!props.selected.length} onClick={props.onClearAll} type="button">
              Clear
            </button>
          ) : null}
          <strong>{open ? "−" : "+"}</strong>
        </div>
      </div>
      {open ? (
        props.items.length ? (
          <>
            {showInlineSearch ? (
              <label className="field compact-field filter-block-search">
                <span>Search</span>
                <input
                  onChange={(event) => setSearch(event.target.value)}
                  placeholder={`Find ${props.title.toLowerCase()}`}
                  value={search}
                />
              </label>
            ) : null}
            <div className="checklist">
              {filteredItems.map((item) => (
              <label key={item}>
                <input checked={selectedSet.has(item)} onChange={() => props.onToggle(item)} type="checkbox" />
                <span>{props.formatLabel ? props.formatLabel(item) : item}</span>
              </label>
              ))}
              {!filteredItems.length ? <p className="muted checklist-empty">No matches for this search.</p> : null}
            </div>
          </>
        ) : (
          <p className="muted">No options available for this filter.</p>
        )
      ) : null}
    </section>
  );
});

const ToggleRow = memo(function ToggleRow(props: { label: string; checked: boolean; onChange: () => void }) {
  return (
    <label className="toggle-row">
      <span className="toggle-label">{props.label}</span>
      <span className={`toggle-switch ${props.checked ? "toggle-switch-on" : ""}`} aria-hidden="true">
        <i />
      </span>
      <input checked={props.checked} onChange={props.onChange} type="checkbox" />
    </label>
  );
});

const DebouncedTextInput = memo(function DebouncedTextInput(props: {
  value: string;
  onCommit: (value: string) => void;
  placeholder?: string;
  inputMode?: "none" | "text" | "tel" | "url" | "email" | "numeric" | "decimal" | "search";
}) {
  const [draft, setDraft] = useState(props.value);
  const debouncedDraft = useDebouncedValue(draft, 180);

  useEffect(() => {
    setDraft(props.value);
  }, [props.value]);

  useEffect(() => {
    if (debouncedDraft !== props.value) {
      props.onCommit(debouncedDraft);
    }
  }, [debouncedDraft, props.onCommit, props.value]);

  return (
    <input
      inputMode={props.inputMode}
      onBlur={() => {
        if (draft !== props.value) {
          props.onCommit(draft);
        }
      }}
      onChange={(event) => setDraft(event.target.value)}
      placeholder={props.placeholder}
      value={draft}
    />
  );
});

const AdvancedNumberField = memo(function AdvancedNumberField(props: { label: string; value: string; onChange: (value: string) => void; placeholder: string }) {
  return (
    <label className="field compact-field">
      <span>{props.label}</span>
      <DebouncedTextInput
        inputMode="numeric"
        onCommit={(value) => props.onChange(value.replace(/[^\d]/g, ""))}
        placeholder={props.placeholder}
        value={props.value}
      />
    </label>
  );
});

function SortableHeader(props: {
  label: string;
  sortKey: string;
  query: LeadQuery;
  onSort: (sortKey: string) => void;
  stickyClass?: string;
}) {
  const active = props.query.sortBy === props.sortKey;
  return (
    <th className={props.stickyClass ?? ""}>
      <button className={`header-button ${active ? "header-active" : ""}`} onClick={() => props.onSort(props.sortKey)} type="button">
        <span>{props.label}</span>
        <small>{active ? (props.query.sortDirection === "desc" ? "↓" : "↑") : "↕"}</small>
      </button>
    </th>
  );
}

function ColumnHeader(props: { column: ColumnKey; query: LeadQuery; onSort: (sortKey: string) => void }) {
  const sortKey = columnSortKey(props.column);
  if (!sortKey) {
    return <th>{columnLabels[props.column]}</th>;
  }
  return <SortableHeader label={columnLabels[props.column]} sortKey={sortKey} query={props.query} onSort={props.onSort} />;
}

function DrawerPillGroup(props: { title: string; items: string[] }) {
  return (
    <div className="drawer-pill-group">
      <span>{props.title}</span>
      <div className="pill-row">{pillList(props.items, 8)}</div>
    </div>
  );
}

function DrawerAccordion(props: { title: string; subtitle?: string; defaultOpen?: boolean; children: ReactNode }) {
  return (
    <details className="drawer-accordion" open={props.defaultOpen}>
      <summary>
        <div className="drawer-accordion-copy">
          <strong>{props.title}</strong>
          {props.subtitle ? <span>{props.subtitle}</span> : null}
        </div>
        <span className="drawer-accordion-icon">Open</span>
      </summary>
      <div className="drawer-accordion-body">{props.children}</div>
    </details>
  );
}

function MiniMetric(props: { label: string; value: string; tone?: string }) {
  return (
    <article className={`mini-metric ${props.tone ? `mini-metric-${props.tone}` : ""}`}>
      <span>{props.label}</span>
      <strong>{props.value}</strong>
    </article>
  );
}

function ContactPreview(props: { title: string; items: string[]; limit?: number }) {
  const limit = props.limit ?? 2;
  return (
    <article className="contact-preview">
      <div className="contact-preview-header">
        <span>{props.title}</span>
        <strong>{props.items.length ? `${props.items.length} found` : "None"}</strong>
      </div>
      {props.items.length ? (
        <ul>
          {props.items.slice(0, limit).map((item) => (
            <li key={item}>{item}</li>
          ))}
        </ul>
      ) : (
        <p className="muted">No data available.</p>
      )}
    </article>
  );
}

function DrawerTimelineChart(props: { rows: TimelineRow[] }) {
  if (!props.rows.length) {
    return <p className="muted">No timeline rows available.</p>;
  }

  const timestamps = props.rows.flatMap((row) => {
    const start = row.first_detected ? Date.parse(row.first_detected) : NaN;
    const end = row.last_found ? Date.parse(row.last_found) : start;
    return [start, end].filter((value) => !Number.isNaN(value));
  });
  if (!timestamps.length) {
    return <p className="muted">Timeline dates are missing for this lead.</p>;
  }

  const minTs = Math.min(...timestamps);
  const maxTs = Math.max(...timestamps);
  const total = Math.max(maxTs - minTs, 1);

  return (
    <div className="drawer-timeline">
      {props.rows.map((row) => {
        const startTs = row.first_detected ? Date.parse(row.first_detected) : minTs;
        const endTs = row.last_found ? Date.parse(row.last_found) : startTs;
        const left = `${((startTs - minTs) / total) * 100}%`;
        const width = `${Math.max(((endTs - startTs) / total) * 100, 1)}%`;
        const colour = colourForToken(row.platform);
        return (
          <div className="drawer-timeline-row" key={`${row.platform}-${row.first_detected}-${row.last_found}`}>
            <div className="drawer-timeline-meta">
              <strong>{humanizeToken(row.platform)}</strong>
              <span>{humanizeToken(row.event_types[0] ?? "timeline")}</span>
            </div>
            <div className="drawer-timeline-dates">
              <span>First seen {formatDate(row.first_detected)}</span>
              <span>Last seen {formatDate(row.last_found, "Now")}</span>
              <span>Last indexed {formatDate(row.last_indexed, "Unknown")}</span>
            </div>
            <div className="drawer-timeline-track">
              <span className="drawer-timeline-span" style={{ left, width, background: colour }} />
            </div>
            <div className="drawer-timeline-badges">
              {row.event_types.map((eventType) => (
                <span className="timeline-event-chip" key={eventType}>
                  {TIMELINE_EVENT_LABELS[eventType as TimelineEventType] ?? humanizeToken(eventType)}
                </span>
              ))}
            </div>
          </div>
        );
      })}
    </div>
  );
}

function MigrationList(props: { migrations: Array<Record<string, string>> }) {
  return (
    <div className="migration-list">
      {props.migrations.map((migration, index) => {
        const likelyDate = midpointDate(migration.last_old_found, migration.first_new_detected);
        const warningFlags = Array.isArray((migration as { warning_flags?: unknown }).warning_flags)
          ? (((migration as unknown as { warning_flags?: string[] }).warning_flags ?? []))
          : [];
        return (
          <article className="migration-card" key={`${migration.old_platform}-${migration.new_platform}-${index}`}>
            <div className="migration-card-header">
              <strong>
                {humanizeToken(migration.old_platform)} → {humanizeToken(migration.new_platform || "unknown")}
              </strong>
              <span>{humanizeToken(migration.migration_status || migration.confidence_level || "matched")}</span>
            </div>
            <p>{migration.migration_reason || "No migration note available."}</p>
            <p>
              Likely migrated around <strong>{formatMonthYear(likelyDate ?? migration.first_new_detected ?? migration.last_old_found)}</strong>
            </p>
            <div className="migration-card-meta">
              <span>Old CMS last seen {formatDate(migration.last_old_found)}</span>
              <span>New CMS first seen {formatDate(migration.first_new_detected)}</span>
              <span>{migration.gap_days ? `${migration.gap_days} day gap` : "Gap not available"}</span>
            </div>
            {warningFlags.length ? (
              <div className="pill-row">
                {warningFlags.slice(0, 3).map((flag) => (
                  <span className="pill signal-pill" key={flag}>
                    {humanizeToken(flag)}
                  </span>
                ))}
              </div>
            ) : null}
          </article>
        );
      })}
    </div>
  );
}

function MigrationHeadlineCard(props: {
  title: string;
  emptyText: string;
  populated: boolean;
  children: ReactNode;
}) {
  return (
    <article className="migration-headline-card">
      <span className="migration-card-label">{props.title}</span>
      {props.populated ? props.children : <p className="muted">{props.emptyText}</p>}
    </article>
  );
}

function StatusBadge(props: { label: string; tone?: string }) {
  return <span className={`status-badge tone-${props.tone ?? "neutral"}`}>{props.label}</span>;
}

function DomainMigrationHeadline(props: { currentDomain: string; bestMatch: Record<string, string> }) {
  return (
    <div className="migration-card-content">
      <strong className="migration-primary-flow">
        {props.currentDomain} ← {props.bestMatch.best_old_domain}
      </strong>
      <div className="drawer-meta">
        <StatusBadge label={humanizeToken(props.bestMatch.domain_migration_status || "unknown")} tone={confidenceTone(props.bestMatch.domain_migration_status)} />
        <StatusBadge
          label={humanizeToken(props.bestMatch.domain_fingerprint_strength || props.bestMatch.fingerprint_strength || "none")}
          tone={confidenceTone(props.bestMatch.domain_fingerprint_strength || props.bestMatch.fingerprint_strength)}
        />
        <StatusBadge label={humanizeToken(props.bestMatch.domain_tld_relationship || "unknown")} />
      </div>
      <p className="migration-copy">{props.bestMatch.domain_migration_reason || props.bestMatch.notes || "No migration summary available."}</p>
      <small>
        Estimated migration date {formatDate(props.bestMatch.domain_migration_estimated_date)} · first redirect seen {formatDate(props.bestMatch.domain_redirect_first_seen)} · last redirect seen {formatDate(props.bestMatch.domain_redirect_last_seen)}
      </small>
    </div>
  );
}

function CmsMigrationHeadline(props: { pair: Record<string, string> }) {
  return (
    <div className="migration-card-content">
      <strong className="migration-primary-flow">
        {humanizeToken(props.pair.old_platform || "unknown")} → {humanizeToken(props.pair.new_platform || "unknown")}
      </strong>
      <div className="drawer-meta">
        <StatusBadge
          label={humanizeToken(props.pair.migration_status || props.pair.confidence_level || "unknown")}
          tone={confidenceTone(props.pair.migration_status || props.pair.confidence_level)}
        />
        <StatusBadge
          label={props.pair.gap_days ? `${props.pair.gap_days} day gap` : "Gap unavailable"}
          tone="neutral"
        />
      </div>
      <p className="migration-copy">{props.pair.migration_reason || "No migration summary available."}</p>
      <p className="migration-copy">
        Likely migrated around <strong>{formatMonthYear(props.pair.likely_migration_date || props.pair.first_new_detected || props.pair.last_old_found)}</strong>
      </p>
      <small>
        Likely migration date {formatDate(props.pair.likely_migration_date)} · old CMS last seen {formatDate(props.pair.last_old_found)} · new CMS first seen {formatDate(props.pair.first_new_detected)}
      </small>
    </div>
  );
}

function SignalChipRow(props: { signals: string[]; emptyText?: string }) {
  const items = props.signals.filter(Boolean);
  if (!items.length) {
    return <p className="muted">{props.emptyText ?? "No shared fingerprint signals captured yet."}</p>;
  }
  return (
    <div className="pill-row">
      {items.slice(0, 8).map((signal) => (
        <span className="pill signal-pill" key={signal}>
          {humanizeSharedSignal(signal)}
        </span>
      ))}
    </div>
  );
}

function TechnologyChipRow(props: { label: string; values: string[] }) {
  if (!props.values.length) {
    return null;
  }
  return (
    <div className="technology-chip-row">
      <span>{props.label}</span>
      <div className="pill-row">
        {props.values.slice(0, 6).map((value) => (
          <span className="pill" key={value}>
            {humanizeToken(value)}
          </span>
        ))}
      </div>
    </div>
  );
}

function ComparisonTile(props: { label: string; value: string }) {
  return (
    <article className="comparison-tile">
      <span>{props.label}</span>
      <strong>{props.value}</strong>
    </article>
  );
}

function DomainCandidateList(props: {
  candidates: Array<Record<string, string> & { shared_signal_flags?: string[]; domain_tld_relationship?: string }>;
}) {
  if (!props.candidates.length) {
    return null;
  }
  return (
    <div className="candidate-list">
      <div className="migration-detail-header">
        <h4>Other possible old domains</h4>
        <span>Top {props.candidates.length}</span>
      </div>
      {props.candidates.map((candidate) => (
        <article className="candidate-card" key={candidate.old_domain}>
          <div className="candidate-card-header">
            <strong>{candidate.old_domain}</strong>
            <div className="drawer-meta">
              <StatusBadge
                label={`${humanizeToken(candidate.enhanced_confidence_band || "unknown")} · ${
                  candidate.enhanced_confidence_score || "—"
                }`}
                tone={confidenceTone(candidate.enhanced_confidence_band)}
              />
              <StatusBadge
                label={humanizeToken(candidate.fingerprint_strength || "none")}
                tone={confidenceTone(candidate.fingerprint_strength)}
              />
            </div>
          </div>
          <div className="candidate-card-meta">
            <span>{humanizeToken(candidate.domain_tld_relationship || "unknown")}</span>
            <span>{candidate.domain_migration_estimated_date ? `Likely ${formatMonthYear(candidate.domain_migration_estimated_date)}` : (candidate.redirect_duration_days ? `${candidate.redirect_duration_days} days observed` : "Redirect window unknown")}</span>
          </div>
          <p className="migration-copy subtle-copy">{candidate.notes || "No summary note available."}</p>
        </article>
      ))}
    </div>
  );
}

function renderCell(
  column: ColumnKey,
  lead: Lead,
  selectedBuckets: string[],
  openScreamingFrogAudit: (rootDomain: string) => void,
) {
  switch (column) {
    case "country":
      return (
        <div className="tight-cell">
          <strong>{humanizeToken(lead.country)}</strong>
          <small>{humanizeToken(lead.geo_confidence)}</small>
        </div>
      );
    case "vertical":
      return <span className="vertical-cell">{lead.vertical || "Unknown"}</span>;
    case "current_platforms":
      return <div className="pill-row">{pillList(lead.current_platforms.length ? lead.current_platforms : lead.current_candidate_platforms, 5)}</div>;
    case "social":
      return <span>{formatNumber(lead.social)}</span>;
    case "sales_revenue":
      return <span>{formatNumber(lead.sales_revenue)}</span>;
    case "employees":
      return <span>{formatNumber(lead.employees)}</span>;
    case "sku":
      return <span>{formatNumber(lead.sku)}</span>;
    case "domain_migration":
      return lead.best_old_domain ? (
        <div className="tight-cell migration-cell">
          <strong>{lead.best_old_domain}</strong>
          <div className="cell-badge-row">
            <StatusBadge label={humanizeToken(lead.domain_migration_status || "unknown")} tone={confidenceTone(lead.domain_migration_status)} />
            <StatusBadge label={humanizeToken(lead.domain_tld_relationship || "unknown")} />
          </div>
          <small>
            {lead.domain_migration_estimated_date
              ? `Likely ${formatMonthYear(lead.domain_migration_estimated_date)}`
              : lead.domain_migration_reason || "No previous-domain summary yet."}
          </small>
        </div>
      ) : (
        <span className="muted">No previous domain</span>
      );
    case "cms_migration":
      return lead.cms_old_platform && lead.cms_new_platform ? (
        <div className="tight-cell migration-cell">
          <strong>
            {humanizeToken(lead.cms_old_platform)} → {humanizeToken(lead.cms_new_platform)}
          </strong>
          <div className="cell-badge-row">
            <StatusBadge label={humanizeToken(lead.cms_migration_status || "unknown")} tone={confidenceTone(lead.cms_migration_status)} />
            <StatusBadge label={humanizeToken(lead.cms_migration_confidence || "unknown")} tone={confidenceTone(lead.cms_migration_confidence)} />
          </div>
          <small>
            {lead.cms_migration_likely_date
              ? `Likely ${formatMonthYear(lead.cms_migration_likely_date)}`
              : lead.cms_migration_gap_days
                ? `${lead.cms_migration_gap_days} day gap`
                : lead.cms_migration_reason || "No CMS migration summary yet."}
          </small>
        </div>
      ) : (
        <span className="muted">No CMS migration</span>
      );
    case "se_market":
      return lead.se_ranking_market ? <strong>{lead.se_ranking_market.toUpperCase()}</strong> : <span className="muted">Not checked</span>;
    case "se_traffic_before":
      return (
        <div className="tight-cell">
          <strong>{formatNumber(lead.se_ranking_traffic_before)}</strong>
          <small>{formatMonthYear(lead.se_ranking_date_label_first || lead.se_ranking_first_month || lead.se_ranking_baseline_month || "")}</small>
        </div>
      );
    case "se_traffic_last_month":
      return (
        <div className="tight-cell">
          <strong>{formatNumber(lead.se_ranking_traffic_last_month)}</strong>
          <small>{formatMonthYear(lead.se_ranking_date_label_second || lead.se_ranking_second_month || lead.se_ranking_comparison_month || "")}</small>
        </div>
      );
    case "se_traffic_change":
      return lead.se_ranking_status ? (
        <div className={`tight-cell se-delta-cell ${seDeltaToneClass(lead.se_ranking_traffic_delta_percent)}`}>
          <strong>{formatPercent(lead.se_ranking_traffic_delta_percent)}</strong>
          <small>{formatSignedNumber(lead.se_ranking_traffic_delta_absolute)} visits</small>
        </div>
      ) : (
        <span className="muted">Not checked</span>
      );
    case "se_keywords_before":
      return <strong>{formatNumber(lead.se_ranking_keywords_before)}</strong>;
    case "se_keywords_last_month":
      return <strong>{formatNumber(lead.se_ranking_keywords_last_month)}</strong>;
    case "se_keyword_change":
      return lead.se_ranking_status ? (
        <div className={`tight-cell se-delta-cell ${seDeltaToneClass(lead.se_ranking_keywords_delta_percent)}`}>
          <strong>{formatPercent(lead.se_ranking_keywords_delta_percent)}</strong>
          <small>{formatSignedNumber(lead.se_ranking_keywords_delta_absolute)} keywords</small>
        </div>
      ) : (
        <span className="muted">Not checked</span>
      );
    case "se_outcome":
      return lead.se_ranking_status ? (
        <div className="tight-cell">
          <div className="pill-row">
            {lead.se_ranking_outcome_flags.length ? lead.se_ranking_outcome_flags.slice(0, 3).map((flag) => (
              <span className="pill signal-pill" key={flag}>
                {humanizeToken(flag)}
              </span>
            )) : <StatusBadge label={humanizeToken(lead.se_ranking_status)} tone={confidenceTone(lead.se_ranking_status)} />}
          </div>
          <small>{lead.se_ranking_error_message || humanizeToken(lead.se_ranking_analysis_mode || lead.se_ranking_analysis_type || "history only")}</small>
        </div>
      ) : (
        <span className="muted">Not checked</span>
      );
    case "se_checked":
      return (
        <div className="tight-cell">
          <strong>{formatDate(lead.se_ranking_checked_at)}</strong>
          <small>{humanizeToken(lead.se_ranking_analysis_mode || lead.se_ranking_analysis_type || "not_checked")}</small>
        </div>
      );
    case "site_status":
      return lead.site_status_category ? (
        <div className="tight-cell">
          <StatusBadge label={humanizeToken(lead.site_status_category)} tone={confidenceTone(lead.site_status_category === "ok" ? "confirmed" : lead.site_status_category === "redirect" ? "possible" : "weak")} />
          <small>{lead.site_status_error || (lead.site_status_final_url ? "Saved site check" : "Status saved")}</small>
        </div>
      ) : (
        <span className="muted">Not checked</span>
      );
    case "site_status_code":
      return lead.site_status_category ? (
        <div className="tight-cell">
          <strong>{lead.site_status_code || "—"}</strong>
          <small>{lead.site_status_redirect_count ? `${lead.site_status_redirect_count} redirects` : "Direct result"}</small>
        </div>
      ) : (
        <span className="muted">—</span>
      );
    case "site_final_url":
      return lead.site_status_final_url ? (
        <a href={lead.site_status_final_url} onClick={(event) => event.stopPropagation()} rel="noopener noreferrer" target="_blank">
          {lead.site_status_final_url}
        </a>
      ) : (
        <span className="muted">No final URL</span>
      );
    case "site_checked":
      return (
        <div className="tight-cell">
          <strong>{formatDate(lead.site_status_checked_at)}</strong>
          <small>{humanizeToken(lead.site_status_category || "not checked")}</small>
        </div>
      );
    case "sf_status":
      return lead.screamingfrog_status ? (
        <div className="tight-cell">
          <StatusBadge label={humanizeToken(lead.screamingfrog_status)} tone={confidenceTone(lead.screamingfrog_status)} />
          <small>{lead.screamingfrog_result_reason === "rate_limited_429" ? "Recrawl slower" : humanizeToken(lead.screamingfrog_crawl_mode || "bounded_audit")}</small>
          <button
            className="ghost-button small-button"
            onClick={(event) => {
              event.stopPropagation();
              openScreamingFrogAudit(lead.root_domain);
            }}
            type="button"
          >
            Open audit
          </button>
        </div>
      ) : (
        <span className="muted">Not audited</span>
      );
    case "sf_config":
      return lead.screamingfrog_status ? (
        <div className="tight-cell">
          <strong>{humanizeToken(lead.screamingfrog_resolved_platform_family || "generic")}</strong>
          <small>{lead.screamingfrog_resolved_config_path ? "Auto selected" : "Fallback"}</small>
        </div>
      ) : (
        <span className="muted">Not audited</span>
      );
    case "sf_quality":
      return lead.screamingfrog_status ? (
        <StatusBadge
          label={lead.screamingfrog_result_reason === "rate_limited_429" ? "Recrawl needed" : humanizeToken(lead.screamingfrog_result_quality || "unknown")}
          tone={lead.screamingfrog_result_reason === "rate_limited_429" ? "warning" : confidenceTone(lead.screamingfrog_result_quality || "neutral")}
        />
      ) : (
        <span className="muted">Not audited</span>
      );
    case "sf_score":
      return lead.screamingfrog_status ? <strong>{formatNumber(lead.screamingfrog_opportunity_score || 0)}</strong> : <span className="muted">—</span>;
    case "sf_primary_issue":
      return lead.screamingfrog_status ? (
        <div className="tight-cell">
          <strong>{humanizeToken(lead.screamingfrog_primary_issue_family || "none")}</strong>
          <small>{lead.screamingfrog_primary_issue_reason || "No primary issue reason"}</small>
        </div>
      ) : (
        <span className="muted">Not audited</span>
      );
    case "sf_checked":
      return (
        <div className="tight-cell">
          <strong>{formatDate(lead.screamingfrog_checked_at)}</strong>
          <small>{lead.screamingfrog_status ? humanizeToken(lead.screamingfrog_crawl_mode || "bounded_audit") : "Not audited"}</small>
        </div>
      );
    case "sf_pages_crawled":
      return lead.screamingfrog_status ? <strong>{formatNumber(lead.screamingfrog_pages_crawled)}</strong> : <span className="muted">—</span>;
    case "sf_homepage_status":
      return lead.screamingfrog_status ? (
        <div className="tight-cell">
          <StatusBadge label={humanizeToken(lead.screamingfrog_homepage_status || "unknown")} tone={confidenceTone(lead.screamingfrog_homepage_status)} />
          <small>{lead.screamingfrog_homepage_status_code || "—"}</small>
        </div>
      ) : (
        <span className="muted">Not audited</span>
      );
    case "sf_title_issues":
      return lead.screamingfrog_status ? <div className="pill-row">{pillList(lead.screamingfrog_title_issue_flags, 4)}</div> : <span className="muted">Not audited</span>;
    case "sf_meta_issues":
      return lead.screamingfrog_status ? <div className="pill-row">{pillList(lead.screamingfrog_meta_issue_flags, 4)}</div> : <span className="muted">Not audited</span>;
    case "sf_canonical_issues":
      return lead.screamingfrog_status ? <div className="pill-row">{pillList(lead.screamingfrog_canonical_issue_flags, 4)}</div> : <span className="muted">Not audited</span>;
    case "sf_internal_errors":
      return lead.screamingfrog_status ? (
        <div className="tight-cell">
          <strong>{`${lead.screamingfrog_internal_4xx_count || 0} / ${lead.screamingfrog_internal_5xx_count || 0}`}</strong>
          <small>{Number(lead.screamingfrog_has_internal_errors || 0) ? "4xx / 5xx found" : "No internal errors"}</small>
        </div>
      ) : (
        <span className="muted">Not audited</span>
      );
    case "sf_strengths": {
      const strengths = screamingFrogStrengths(lead);
      return lead.screamingfrog_status ? (
        strengths.length ? <div className="pill-row">{pillList(strengths, 4)}</div> : <span className="muted">No standout segmentation strengths</span>
      ) : (
        <span className="muted">Not audited</span>
      );
    }
    case "sf_issue_signals": {
      const issues = screamingFrogIssueSignals(lead);
      return lead.screamingfrog_status ? (
        issues.length ? <div className="pill-row">{pillList(issues, 4)}</div> : <span className="muted">No major issue flags</span>
      ) : (
        <span className="muted">Not audited</span>
      );
    }
    case "sf_heading_health": {
      const headingHealth = screamingFrogHeadingHealth(lead);
      return lead.screamingfrog_status ? (
        <div className="tight-cell">
          <StatusBadge label={headingHealth.label} tone={headingHealth.tone} />
          <small>{headingHealth.note}</small>
        </div>
      ) : (
        <span className="muted">Not audited</span>
      );
    }
    case "sf_evidence": {
      const evidence = screamingFrogEvidenceGrade(lead);
      return lead.screamingfrog_status ? (
        <div className="tight-cell">
          <StatusBadge label={evidence.label} tone={evidence.tone} />
          <small>{evidence.note}</small>
        </div>
      ) : (
        <span className="muted">Not audited</span>
      );
    }
    case "sf_location_pages":
      return lead.screamingfrog_status ? <strong>{formatNumber(lead.screamingfrog_location_page_count)}</strong> : <span className="muted">—</span>;
    case "sf_service_pages":
      return lead.screamingfrog_status ? <strong>{formatNumber(lead.screamingfrog_service_page_count)}</strong> : <span className="muted">—</span>;
    case "sf_collection_detection":
      return lead.screamingfrog_status ? (
        <div className="tight-cell">
          <strong>{humanizeToken(lead.screamingfrog_collection_detection_status || "unknown")}</strong>
          <small>{`${formatNumber(lead.screamingfrog_collection_detection_confidence || 0)}% confidence`}</small>
        </div>
      ) : (
        <span className="muted">Not audited</span>
      );
    case "sf_collection_intro":
      return lead.screamingfrog_status ? (
        <div className="tight-cell">
          <strong>{humanizeToken(lead.screamingfrog_collection_intro_status || "unknown")}</strong>
          <small>{lead.screamingfrog_collection_issue_family === "collection_page_not_reviewable"
            ? "No reliable collection/category page captured"
            : `${formatNumber(lead.screamingfrog_collection_best_intro_confidence || lead.screamingfrog_collection_intro_confidence || 0)}% · ${lead.screamingfrog_collection_issue_reason || "No collection read yet"}`}</small>
        </div>
      ) : (
        <span className="muted">Not audited</span>
      );
    case "sf_collection_snippet": {
      const snippet = screamingFrogCollectionSnippet(lead);
      return lead.screamingfrog_status ? (
        <div className="tight-cell">
          <strong>{snippet.label}</strong>
          <small>{snippet.note}</small>
        </div>
      ) : (
        <span className="muted">Not audited</span>
      );
    }
    case "sf_collection_title_signal": {
      const titleSignal = screamingFrogCollectionTitleSignal(lead);
      return lead.screamingfrog_status ? (
        <div className="tight-cell">
          <StatusBadge label={titleSignal.label} tone={titleSignal.tone} />
          <small>{titleSignal.note}</small>
        </div>
      ) : (
        <span className="muted">Not audited</span>
      );
    }
    case "sf_collection_confidence": {
      const confidence = screamingFrogCollectionConfidence(lead);
      return lead.screamingfrog_status ? (
        <div className="tight-cell">
          <StatusBadge label={confidence.label} tone={confidence.tone} />
          <small>{confidence.note}</small>
        </div>
      ) : (
        <span className="muted">Not audited</span>
      );
    }
    case "sf_title_optimization":
      return lead.screamingfrog_status ? (
        <StatusBadge label={humanizeToken(lead.screamingfrog_title_optimization_status || "unknown")} tone={confidenceTone(lead.screamingfrog_title_optimization_status || "neutral")} />
      ) : (
        <span className="muted">Not audited</span>
      );
    case "sf_collection_products":
      return lead.screamingfrog_status ? <strong>{formatNumber(lead.screamingfrog_collection_product_count)}</strong> : <span className="muted">—</span>;
    case "sf_collection_schema":
      return lead.screamingfrog_status ? <div className="pill-row">{pillList(lead.screamingfrog_collection_schema_types, 4)}</div> : <span className="muted">Not audited</span>;
    case "domain_fingerprint_strength":
      return (
        <StatusBadge
          label={humanizeToken(lead.domain_fingerprint_strength || "none")}
          tone={confidenceTone(lead.domain_fingerprint_strength)}
        />
      );
    case "domain_shared_signals":
      return lead.domain_migration_evidence_flags.length ? (
        <div className="pill-row">
          {lead.domain_migration_evidence_flags.slice(0, 4).map((signal) => (
            <span className="pill signal-pill" key={signal}>
              {humanizeSharedSignal(signal)}
            </span>
          ))}
        </div>
      ) : lead.domain_shared_signals.length ? (
        <div className="pill-row">
          {lead.domain_shared_signals.slice(0, 4).map((signal) => (
            <span className="pill signal-pill" key={signal}>
              {humanizeSharedSignal(signal)}
            </span>
          ))}
        </div>
      ) : lead.domain_shared_technologies.length ? (
        <div className="pill-row">
          {lead.domain_shared_technologies.slice(0, 4).map((signal) => (
            <span className="pill signal-pill" key={signal}>
              {humanizeToken(signal)}
            </span>
          ))}
        </div>
      ) : (
        <span className="muted">No shared signals</span>
      );
    case "removed_platforms":
      return <div className="pill-row">{pillList(lead.removed_platforms, 5)}</div>;
    case "matched_timeline_platforms":
      return <div className="pill-row">{pillList(lead.matched_timeline_platforms, 5)}</div>;
    case "matched_first_detected":
      return (
        <div className="tight-cell">
          <strong>{formatDate(lead.matched_first_detected)}</strong>
          <small>Tech first seen for {lead.matched_timeline_platforms.map(humanizeToken).join(", ") || "matched CMS"}</small>
        </div>
      );
    case "matched_last_found":
      return (
        <div className="tight-cell">
          <strong>{formatDate(lead.matched_last_found)}</strong>
          <small>Tech last seen in BuiltWith</small>
        </div>
      );
    case "cms_migration_date":
      return (
        <div className="tight-cell">
          <strong>{formatDate(lead.cms_migration_likely_date)}</strong>
          <small>{lead.cms_old_platform && lead.cms_new_platform ? `${humanizeToken(lead.cms_old_platform)} → ${humanizeToken(lead.cms_new_platform)}` : "No CMS migration date"}</small>
        </div>
      );
    case "domain_migration_date":
      return (
        <div className="tight-cell">
          <strong>{formatDate(lead.domain_migration_estimated_date)}</strong>
          <small>{lead.best_old_domain ? `From ${lead.best_old_domain}` : "No domain migration date"}</small>
        </div>
      );
    case "sales_buckets":
      return <div className="pill-row">{pillList(lead.sales_buckets, 4)}</div>;
    case "contact_status":
      return (
        <div className="contact-status">
          <span>{lead.contact_status.hasEmail ? "Email" : "No email"}</span>
          <span>{lead.contact_status.hasPhone ? "Phone" : "No phone"}</span>
          <span>{lead.contact_status.hasPeople ? "People" : "No people"}</span>
        </div>
      );
    case "technology_spend":
      return <span>{formatNumber(lead.technology_spend)}</span>;
    case "total_score":
      return (
        <div className="tight-cell">
          <strong>{lead.total_score}</strong>
          <small>c{lead.contact_score} · s{lead.stack_score} · t{lead.trigger_score}</small>
        </div>
      );
    case "priority_tier":
      return <span className={`tier-badge tier-${lead.priority_tier}`}>{lead.priority_tier}</span>;
    case "marketing_platforms":
      return <div className="pill-row">{pillList(lead.marketing_platforms, 4)}</div>;
    case "crm_platforms":
      return <div className="pill-row">{pillList(lead.crm_platforms, 4)}</div>;
    case "payment_platforms":
      return <div className="pill-row">{pillList(lead.payment_platforms, 4)}</div>;
    case "hosting_providers":
      return <div className="pill-row">{pillList(lead.hosting_providers, 4)}</div>;
    case "agencies":
      return <div className="pill-row">{pillList(lead.agencies, 4)}</div>;
    case "ai_tools":
      return <div className="pill-row">{pillList(lead.ai_tools, 4)}</div>;
    case "compliance_flags":
      return <div className="pill-row">{pillList(lead.compliance_flags, 4)}</div>;
    case "reason":
      return <span className="reason-cell">{matchingReasonText(lead, selectedBuckets)}</span>;
    default:
      return null;
  }
}
