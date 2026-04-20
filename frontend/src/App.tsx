import { startTransition, useDeferredValue, useEffect, useMemo, useState, type ReactNode } from "react";
import {
  addToExportTray,
  clearExportTray,
  createPreset,
  deletePreset,
  exportLeadUrl,
  fetchAnalytics,
  fetchExportTray,
  fetchFilterOptions,
  fetchLeadDetail,
  fetchLeads,
  fetchPresets,
  fetchSeRankingSummary,
  fetchSummary,
  fetchTimelineCohort,
  refreshSeRankingAnalysis,
  removeFromExportTray,
  runSeRankingAnalysis,
  updatePreset,
} from "./api";
import type {
  AnalyticsResponse,
  ExportTrayResponse,
  FilterOptions,
  Lead,
  LeadDetailResponse,
  LeadQuery,
  LeadsResponse,
  MigrationTimingOperator,
  Preset,
  SeRankingSummaryResponse,
  SummaryResponse,
  TimelineCohortResponse,
  TimelineDateField,
  TimelineEventType,
  TimelineGranularity,
  TimelineRow,
} from "./types";

type ColumnKey =
  | "country"
  | "vertical"
  | "current_platforms"
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
  | "reason"
  | "se_market"
  | "se_traffic_before"
  | "se_traffic_last_month"
  | "se_traffic_change"
  | "se_keywords_before"
  | "se_keywords_last_month"
  | "se_keyword_change"
  | "se_outcome"
  | "se_checked";

type SeRankingAnalysisType = "cms_migration" | "domain_migration";

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
};

const PLATFORM_COLOURS: Record<string, string> = {
  shopify: "#155eef",
  shopify_plus: "#1d4ed8",
  woocommerce_checkout: "#7c3aed",
  bigcommerce: "#0f7b6c",
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
  "domain_migration",
  "cms_migration",
  "domain_migration_date",
  "cms_migration_date",
  "domain_fingerprint_strength",
  "domain_shared_signals",
  "sales_buckets",
  "contact_status",
  "technology_spend",
  "total_score",
  "priority_tier",
  "reason",
];

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
  selectedOnly: false,
  hasSeRankingAnalysis: false,
  seRankingAnalysisTypes: [],
  seRankingOutcomeFlags: [],
  timelinePlatforms: [],
  timelineEventTypes: DEFAULT_TIMELINE_EVENT_TYPES,
  timelineDateField: "first_seen",
  timelineSeenFrom: "",
  timelineSeenTo: "",
  cmsMigrationFrom: "",
  cmsMigrationTo: "",
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
  domain_migration: "Previous domain candidate",
  cms_migration: "Possible CMS migration",
  se_market: "SE market",
  se_traffic_before: "Traffic before",
  se_traffic_last_month: "Traffic last month",
  se_traffic_change: "Traffic change",
  se_keywords_before: "Keywords before",
  se_keywords_last_month: "Keywords last month",
  se_keyword_change: "Keyword change",
  se_outcome: "Outcome",
  se_checked: "SE checked",
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
  reason: "Why this lead",
};

function toggle(values: string[], value: string) {
  return values.includes(value) ? values.filter((item) => item !== value) : [...values, value];
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
    hasDomainMigration: raw?.hasDomainMigration ?? initialQuery.hasDomainMigration,
    hasCmsMigration: raw?.hasCmsMigration ?? initialQuery.hasCmsMigration,
    domainMigrationStatuses: raw?.domainMigrationStatuses ?? initialQuery.domainMigrationStatuses,
    domainConfidenceBands: raw?.domainConfidenceBands ?? initialQuery.domainConfidenceBands,
    domainFingerprintStrengths: raw?.domainFingerprintStrengths ?? initialQuery.domainFingerprintStrengths,
    domainTldRelationships: raw?.domainTldRelationships ?? initialQuery.domainTldRelationships,
    cmsMigrationStatuses: raw?.cmsMigrationStatuses ?? initialQuery.cmsMigrationStatuses,
    cmsConfidenceLevels: raw?.cmsConfidenceLevels ?? initialQuery.cmsConfidenceLevels,
    hasSeRankingAnalysis: raw?.hasSeRankingAnalysis ?? initialQuery.hasSeRankingAnalysis,
    seRankingAnalysisTypes: raw?.seRankingAnalysisTypes ?? initialQuery.seRankingAnalysisTypes,
    seRankingOutcomeFlags: raw?.seRankingOutcomeFlags ?? initialQuery.seRankingOutcomeFlags,
    timelinePlatforms: raw?.timelinePlatforms ?? initialQuery.timelinePlatforms,
    timelineEventTypes: (raw?.timelineEventTypes as TimelineEventType[] | undefined) ?? initialQuery.timelineEventTypes,
    timelineDateField: (raw?.timelineDateField as TimelineDateField | undefined) ?? initialQuery.timelineDateField,
    timelineSeenFrom: raw?.timelineSeenFrom ?? legacyRaw?.startedFrom ?? initialQuery.timelineSeenFrom,
    timelineSeenTo: raw?.timelineSeenTo ?? legacyRaw?.startedTo ?? initialQuery.timelineSeenTo,
    cmsMigrationFrom: raw?.cmsMigrationFrom ?? initialQuery.cmsMigrationFrom,
    cmsMigrationTo: raw?.cmsMigrationTo ?? initialQuery.cmsMigrationTo,
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
  return sanitized.length ? sanitized : defaultVisibleColumns;
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
  if (normalized === "high" || normalized === "strong" || normalized === "confirmed") {
    return "positive";
  }
  if (normalized === "medium" || normalized === "moderate" || normalized === "possible" || normalized === "probable") {
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
  if (normalized === "error" || normalized === "down") {
    return "warning";
  }
  if (normalized === "success" || normalized === "up") {
    return "positive";
  }
  return "neutral";
}

function pillList(values: string[], limit = 3) {
  if (!values.length) {
    return <span className="muted">None</span>;
  }
  return values.slice(0, limit).map((value) => (
    <span className="pill" key={value}>
      {humanizeToken(value)}
    </span>
  ));
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
    case "se_checked":
      return "se_ranking_checked_at";
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

function quickRange(months: 3 | 6 | 12 | 24) {
  const end = new Date();
  const start = new Date(end);
  start.setMonth(end.getMonth() - months);
  return {
    from: start.toISOString().slice(0, 10),
    to: end.toISOString().slice(0, 10),
  };
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

function applyDateWindow(
  patchKeyFrom: keyof LeadQuery,
  patchKeyTo: keyof LeadQuery,
  range: 3 | 6 | 12 | 24 | "all",
): Partial<LeadQuery> {
  if (range === "all") {
    return { [patchKeyFrom]: "", [patchKeyTo]: "" } as Partial<LeadQuery>;
  }
  const next = quickRange(range);
  return { [patchKeyFrom]: next.from, [patchKeyTo]: next.to } as Partial<LeadQuery>;
}

function quickRangeSelection(from: string, to: string) {
  if (!from && !to) {
    return "all";
  }
  for (const option of [3, 6, 12, 24] as const) {
    const range = quickRange(option);
    if (from === range.from && to === range.to) {
      return `${option}`;
    }
  }
  return "custom";
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
  const [analytics, setAnalytics] = useState<AnalyticsResponse | null>(null);
  const [timeline, setTimeline] = useState<TimelineCohortResponse | null>(null);
  const [tray, setTray] = useState<ExportTrayResponse | null>(null);
  const [selectedLeadId, setSelectedLeadId] = useState<string | null>(null);
  const [detail, setDetail] = useState<LeadDetailResponse | null>(null);
  const [currentPresetId, setCurrentPresetId] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [analyticsLoading, setAnalyticsLoading] = useState(true);
  const [timelineLoading, setTimelineLoading] = useState(false);
  const [detailLoading, setDetailLoading] = useState(false);
  const [error, setError] = useState("");
  const [toast, setToast] = useState("");
  const [searchDraft, setSearchDraft] = useState(initialQuery.search);
  const [verticalSearch, setVerticalSearch] = useState("");
  const [drawerPending, setDrawerPending] = useState<"first" | "last" | null>(null);
  const [showColumnChooser, setShowColumnChooser] = useState(false);
  const [presetNameDraft, setPresetNameDraft] = useState("");
  const [presetModal, setPresetModal] = useState<null | { mode: "save" | "delete" }>(null);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [sidebarSections, setSidebarSections] = useState({
    search: true,
    common: true,
    migration: true,
    advanced: false,
  });
  const [analyticsExpanded, setAnalyticsExpanded] = useState(false);
  const [trayCollapsed, setTrayCollapsed] = useState(true);
  const [spreadsheetFocusMode, setSpreadsheetFocusMode] = useState(false);
  const [browserFullscreen, setBrowserFullscreen] = useState(false);
  const [seRankingType, setSeRankingType] = useState<SeRankingAnalysisType>("cms_migration");
  const [seRankingSummary, setSeRankingSummary] = useState<SeRankingSummaryResponse | null>(null);
  const [seRankingLoading, setSeRankingLoading] = useState(false);

  const deferredSearch = useDeferredValue(query.search);
  const deferredVerticalSearch = useDeferredValue(verticalSearch);
  const filteredVerticalOptions = (options?.verticals ?? []).filter((vertical) =>
    vertical.toLowerCase().includes(deferredVerticalSearch.trim().toLowerCase()),
  );
  const traySet = useMemo(() => new Set(tray?.rootDomains ?? []), [tray]);
  const traySignature = useMemo(() => (tray?.rootDomains ?? []).join("|"), [tray]);
  const activePreset = presets.find((preset) => preset.id === currentPresetId) ?? null;
  const selectedLead = leads?.items.find((item) => item.root_domain === selectedLeadId) ?? null;
  const hasTimelineSelection = query.timelinePlatforms.length > 0;
  const hasCmsTiming = Boolean(query.cmsMigrationFrom || query.cmsMigrationTo);
  const hasDomainTiming = Boolean(query.domainMigrationFrom || query.domainMigrationTo);
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
    const normalizedVisibleColumns = ensureColumns(visibleColumns);
    if (!hasTimelineSelection) {
      return normalizedVisibleColumns;
    }
    const required: ColumnKey[] = ["matched_timeline_platforms", "matched_first_detected", "matched_last_found"];
    return [...normalizedVisibleColumns, ...required.filter((column) => !normalizedVisibleColumns.includes(column))];
  }, [hasTimelineSelection, visibleColumns]);
  const groupedPresets = useMemo(() => {
    const groups = new Map<string, Preset[]>();
    presets.forEach((preset) => {
      const list = groups.get(preset.group) ?? [];
      list.push(preset);
      groups.set(preset.group, list);
    });
    return Array.from(groups.entries());
  }, [presets]);

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
    const syncFullscreen = () => {
      setBrowserFullscreen(Boolean(document.fullscreenElement));
    };
    syncFullscreen();
    document.addEventListener("fullscreenchange", syncFullscreen);
    return () => document.removeEventListener("fullscreenchange", syncFullscreen);
  }, []);

  useEffect(() => {
    document.title = "DOMAIN DEALER";
  }, []);

  useEffect(() => {
    let cancelled = false;
    async function loadSeRankingSummary() {
      if (!tray?.count) {
        setSeRankingSummary(null);
        setSeRankingLoading(false);
        return;
      }
      setSeRankingLoading(true);
      try {
        const response = await fetchSeRankingSummary(seRankingType);
        if (!cancelled) {
          setSeRankingSummary(response);
        }
      } catch (loadError) {
        if (!cancelled) {
          setError(loadError instanceof Error ? loadError.message : "Failed to load SE Ranking summary");
        }
      } finally {
        if (!cancelled) {
          setSeRankingLoading(false);
        }
      }
    }
    void loadSeRankingSummary();
    return () => {
      cancelled = true;
    };
  }, [seRankingType, traySignature]);

  function updateQuery(patch: Partial<LeadQuery>, keepPreset = false) {
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
  }

  const activeFilterChips = [
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
      ? [{ group: "Tech timing", label: "Last seen", clear: () => updateQuery({ timelineDateField: "first_seen" }) }]
      : []),
    ...(hasTimelineSelection && query.timelineSeenFrom
      ? [{ group: "Tech timing", label: `From ${query.timelineSeenFrom}`, clear: () => updateQuery({ timelineSeenFrom: "" }) }]
      : []),
    ...(hasTimelineSelection && query.timelineSeenTo
      ? [{ group: "Tech timing", label: `To ${query.timelineSeenTo}`, clear: () => updateQuery({ timelineSeenTo: "" }) }]
      : []),
    ...(hasTimelineSelection && query.timelineGranularity !== "month"
      ? [{ group: "Timeline", label: humanizeToken(query.timelineGranularity), clear: () => updateQuery({ timelineGranularity: "month" }) }]
      : []),
    ...(query.cmsMigrationFrom
      ? [{ group: "CMS migration", label: `From ${query.cmsMigrationFrom}`, clear: () => updateQuery({ cmsMigrationFrom: "" }) }]
      : []),
    ...(query.cmsMigrationTo
      ? [{ group: "CMS migration", label: `To ${query.cmsMigrationTo}`, clear: () => updateQuery({ cmsMigrationTo: "" }) }]
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
  ];

  useEffect(() => {
    let cancelled = false;
    async function loadBoot() {
      try {
        const [summaryResponse, presetsResponse, trayResponse] = await Promise.all([fetchSummary(), fetchPresets(), fetchExportTray()]);
        if (!cancelled) {
          setSummary(summaryResponse);
          setPresets(presetsResponse.items);
          setTray(trayResponse);
        }
        const optionsResponse = await fetchFilterOptions();
        if (!cancelled) {
          setOptions(optionsResponse);
        }
      } catch (loadError) {
        if (!cancelled) {
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
    setLoading(true);
    setError("");
    async function loadLeadsOnly() {
      try {
        const effectiveQuery = normalizeLeadQuery({ ...query, search: deferredSearch });
        const leadResponse = await fetchLeads(effectiveQuery);
        if (cancelled) {
          return;
        }
        setLeads(leadResponse);

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
          setError(loadError instanceof Error ? loadError.message : "Failed to load leads");
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
  }, [
    deferredSearch,
    query.exactDomain,
    query.countries,
    query.tiers,
    query.currentPlatforms,
    query.recentPlatforms,
    query.removedPlatforms,
    query.hasDomainMigration,
    query.hasCmsMigration,
    query.domainMigrationStatuses,
    query.domainConfidenceBands,
    query.domainFingerprintStrengths,
    query.domainTldRelationships,
    query.cmsMigrationStatuses,
    query.cmsConfidenceLevels,
    query.verticals,
    query.salesBuckets,
    query.timelinePlatforms,
    query.timelineEventTypes,
    query.timelineDateField,
    query.timelineSeenFrom,
    query.timelineSeenTo,
    query.cmsMigrationFrom,
    query.cmsMigrationTo,
    query.domainMigrationFrom,
    query.domainMigrationTo,
    query.migrationTimingOperator,
    query.timelineGranularity,
    query.migrationOnly,
    query.hasContact,
    query.hasMarketing,
    query.hasCrm,
    query.hasPayments,
    query.hasSeRankingAnalysis,
    query.seRankingAnalysisTypes,
    query.seRankingOutcomeFlags,
    query.selectedOnly,
    query.page,
    query.pageSize,
    query.sortBy,
    query.sortDirection,
    drawerPending,
  ]);

  useEffect(() => {
    let cancelled = false;
    setAnalyticsLoading(true);
    setTimelineLoading(true);
    async function loadSupportingPanels() {
      try {
        const effectiveQuery = normalizeLeadQuery({ ...query, search: deferredSearch });
        const [analyticsResponse, timelineResponse] = await Promise.all([
          fetchAnalytics({ ...effectiveQuery, page: 1 }),
          fetchTimelineCohort({ ...effectiveQuery, page: 1 }),
        ]);
        if (!cancelled) {
          setAnalytics(analyticsResponse);
          setTimeline(timelineResponse);
        }
      } catch (loadError) {
        if (!cancelled) {
          setError(loadError instanceof Error ? loadError.message : "Failed to load analytics");
        }
      } finally {
        if (!cancelled) {
          setAnalyticsLoading(false);
          setTimelineLoading(false);
        }
      }
    }
    void loadSupportingPanels();
    return () => {
      cancelled = true;
    };
  }, [
    deferredSearch,
    query.exactDomain,
    query.countries,
    query.tiers,
    query.currentPlatforms,
    query.recentPlatforms,
    query.removedPlatforms,
    query.hasDomainMigration,
    query.hasCmsMigration,
    query.domainMigrationStatuses,
    query.domainConfidenceBands,
    query.domainFingerprintStrengths,
    query.domainTldRelationships,
    query.cmsMigrationStatuses,
    query.cmsConfidenceLevels,
    query.verticals,
    query.salesBuckets,
    query.timelinePlatforms,
    query.timelineEventTypes,
    query.timelineDateField,
    query.timelineSeenFrom,
    query.timelineSeenTo,
    query.cmsMigrationFrom,
    query.cmsMigrationTo,
    query.domainMigrationFrom,
    query.domainMigrationTo,
    query.migrationTimingOperator,
    query.timelineGranularity,
    query.migrationOnly,
    query.hasContact,
    query.hasMarketing,
    query.hasCrm,
    query.hasPayments,
    query.hasSeRankingAnalysis,
    query.seRankingAnalysisTypes,
    query.seRankingOutcomeFlags,
    query.selectedOnly,
  ]);

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

  async function handleUpdatePreset() {
    if (!activePreset || activePreset.isBuiltin) {
      return;
    }
    try {
      await updatePreset(activePreset.id, {
        name: activePreset.name,
        filters: normalizeLeadQuery({ ...query, page: 1 }),
        visibleColumns,
        sort: { sortBy: query.sortBy, sortDirection: query.sortDirection },
      });
      await refreshPresets();
    } catch (updateError) {
      setError(updateError instanceof Error ? updateError.message : "Failed to update preset");
    }
  }

  async function handleDeletePreset() {
    if (!activePreset || activePreset.isBuiltin) {
      return;
    }
    setPresetModal({ mode: "delete" });
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

  async function clearTraySelection() {
    try {
      setTray(await clearExportTray());
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

  function exportSingleLead(detailResponse: LeadDetailResponse) {
    const values = {
      root_domain: detailResponse.exportReady.root_domain,
      company: detailResponse.exportReady.company,
      country: detailResponse.exportReady.country,
      best_old_domain: detailResponse.lead.best_old_domain,
      domain_migration_confidence: detailResponse.lead.domain_migration_confidence_band,
      cms_migration: detailResponse.lead.cms_migration_summary,
      se_market: detailResponse.lead.se_ranking_market,
      se_traffic_before: detailResponse.lead.se_ranking_traffic_before,
      se_traffic_last_month: detailResponse.lead.se_ranking_traffic_last_month,
      se_traffic_delta_percent: detailResponse.lead.se_ranking_traffic_delta_percent,
      se_keywords_before: detailResponse.lead.se_ranking_keywords_before,
      se_keywords_last_month: detailResponse.lead.se_ranking_keywords_last_month,
      se_keywords_delta_percent: detailResponse.lead.se_ranking_keywords_delta_percent,
      se_outcome_flags: detailResponse.lead.se_ranking_outcome_flags.join(" | "),
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

  function handleTimelineQuickRange(range: 3 | 6 | 12 | 24 | "all") {
    updateQuery(applyDateWindow("timelineSeenFrom", "timelineSeenTo", range));
  }

  function handleCmsMigrationQuickRange(range: 3 | 6 | 12 | 24 | "all") {
    updateQuery(applyDateWindow("cmsMigrationFrom", "cmsMigrationTo", range));
  }

  function handleDomainMigrationQuickRange(range: 3 | 6 | 12 | 24 | "all") {
    updateQuery(applyDateWindow("domainMigrationFrom", "domainMigrationTo", range));
  }

  function toggleTimelineEventType(eventType: TimelineEventType) {
    const next = query.timelineEventTypes.includes(eventType)
      ? query.timelineEventTypes.filter((value) => value !== eventType)
      : [...query.timelineEventTypes, eventType];
    updateQuery({ timelineEventTypes: next as TimelineEventType[] });
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

  function clearDomainMigrationTiming() {
    updateQuery({ domainMigrationFrom: "", domainMigrationTo: "" });
  }

  function clearAllMigrationTiming() {
    updateQuery({
      cmsMigrationFrom: "",
      cmsMigrationTo: "",
      domainMigrationFrom: "",
      domainMigrationTo: "",
      migrationTimingOperator: "and",
    });
  }

  async function reloadWorksheet() {
    await refreshTray();
    const summary = await fetchSeRankingSummary(seRankingType);
    setSeRankingSummary(summary);
    setQuery((current) => normalizeLeadQuery({ ...current }));
    if (selectedLeadId) {
      setDetail(await fetchLeadDetail(selectedLeadId));
    }
  }

  async function handleRunSeRankingAnalysis(confirm = false) {
    try {
      setSeRankingLoading(true);
      const response = await runSeRankingAnalysis(seRankingType, confirm);
      setSeRankingSummary({ analysisType: response.analysisType, summary: response.summary });
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
      const response = await refreshSeRankingAnalysis(seRankingType);
      setSeRankingSummary({ analysisType: response.analysisType, summary: response.summary });
      await reloadWorksheet();
      setToast(`SE Ranking refreshed ${response.results.length} domains`);
    } catch (refreshError) {
      setError(refreshError instanceof Error ? refreshError.message : "Failed to refresh SE Ranking analysis");
    } finally {
      setSeRankingLoading(false);
    }
  }

  const techTimingPresetValue = quickRangeSelection(query.timelineSeenFrom, query.timelineSeenTo);
  const cmsTimingPresetValue = quickRangeSelection(query.cmsMigrationFrom, query.cmsMigrationTo);
  const domainTimingPresetValue = quickRangeSelection(query.domainMigrationFrom, query.domainMigrationTo);
  const hasMigrationTiming = hasCmsTiming || hasDomainTiming;
  const techTimingSummary = query.timelinePlatforms.length
    ? `${query.timelineDateField === "last_seen" ? "Last seen" : "First seen"} · ${dateWindowLabel(query.timelineSeenFrom, query.timelineSeenTo)}`
    : "No technology timing filter";
  const cmsTimingSummary = hasCmsTiming
    ? dateWindowLabel(query.cmsMigrationFrom, query.cmsMigrationTo)
    : "All CMS migration dates";
  const domainTimingSummary = hasDomainTiming
    ? dateWindowLabel(query.domainMigrationFrom, query.domainMigrationTo)
    : "All domain migration dates";
  const searchSectionCount = Number(Boolean(query.search || query.exactDomain));
  const commonSectionCount =
    query.verticals.length +
    (options && query.countries.length !== options.countries.length ? query.countries.length : 0) +
    (options && query.tiers.length !== options.tiers.length ? query.tiers.length : 0) +
    query.salesBuckets.length +
    query.currentPlatforms.length +
    query.removedPlatforms.length +
    query.recentPlatforms.length +
    Number(query.hasDomainMigration) +
    Number(query.hasCmsMigration) +
    Number(query.hasContact) +
    Number(query.hasSeRankingAnalysis) +
    Number(query.selectedOnly);
  const migrationSectionCount =
    query.domainMigrationStatuses.length +
    query.domainConfidenceBands.length +
    query.domainFingerprintStrengths.length +
    query.domainTldRelationships.length +
    query.cmsMigrationStatuses.length +
    query.cmsConfidenceLevels.length +
    query.seRankingAnalysisTypes.length +
    query.seRankingOutcomeFlags.length;
  const advancedSectionCount =
    Number(query.migrationOnly) +
    Number(query.hasMarketing) +
    Number(query.hasCrm) +
    Number(query.hasPayments);

  return (
    <div className={appShellClassName}>
      <header className="topbar">
        <div className="topbar-brand-block">
          <div className="brand-lockup">
            <div className="brand-mark">DD</div>
            <div className="brand-copy">
              <p className="kicker">Migration intelligence for outbound</p>
              <h1>DOMAIN DEALER</h1>
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
          </div>
        </div>
        <div className="header-actions">
          <a className="primary-button" href={exportLeadUrl(query)}>
            Export filtered CSV
          </a>
          <button className="ghost-button" onClick={handleSavePreset} type="button">
            Save preset
          </button>
          <div className="stamp-card">
            <span>Processed</span>
            <strong>{summary ? new Date(summary.processed_at).toLocaleString() : "Loading…"}</strong>
            <small>{summary ? `${summary.overview.unique_leads.toLocaleString()} scoped leads` : ""}</small>
          </div>
        </div>
      </header>

      <section className="preset-strip">
        <div className="preset-header compact-header">
          <div>
            <h2>Workspace views</h2>
            <p>Switch your prospecting lens, then refine from the shell rail.</p>
          </div>
          <div className="preset-actions">
            {activePreset && !activePreset.isBuiltin ? (
              <>
                <button className="ghost-button small-button" onClick={handleUpdatePreset} type="button">
                  Update preset
                </button>
                <button className="ghost-button small-button" onClick={handleDeletePreset} type="button">
                  Delete preset
                </button>
              </>
            ) : null}
          </div>
        </div>
        <div className="compact-toolbar-row">
          <label className="field compact-field preset-select-field">
            <span>Preset</span>
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
          <div className="compact-preset-summary">
            <strong>{activePreset?.name ?? "Current custom view"}</strong>
            <span>{activePreset?.description ?? "Discovery mode across the full DOMAIN DEALER workspace."}</span>
          </div>
        </div>
      </section>

      <section className="timeline-panel">
        <div className="timeline-panel-header compact-header">
          <div>
            <h2>Technology timing</h2>
            <p>Track platform recency, start signals, and migration windows without leaving the worksheet.</p>
          </div>
        </div>
        <div className="compact-toolbar-row timeline-compact-row">
          <label className="field compact-field timeline-range-field">
            <span>Tech timing preset</span>
            <select
              value={techTimingPresetValue}
              onChange={(event) => {
                const value = event.target.value;
                if (value === "all") handleTimelineQuickRange("all");
                else if (value === "3") handleTimelineQuickRange(3);
                else if (value === "6") handleTimelineQuickRange(6);
                else if (value === "12") handleTimelineQuickRange(12);
                else if (value === "24") handleTimelineQuickRange(24);
              }}
            >
              <option value="all">All time</option>
              <option value="3">Past 3 months</option>
              <option value="6">Past 6 months</option>
              <option value="12">Past 12 months</option>
              <option value="24">Past 24 months</option>
              <option value="custom">Custom dates below</option>
            </select>
          </label>
          <label className="field compact-field timeline-range-field">
            <span>Granularity</span>
            <select
              value={query.timelineGranularity}
              onChange={(event) => updateQuery({ timelineGranularity: event.target.value as TimelineGranularity })}
            >
              <option value="week">Weekly</option>
              <option value="month">Monthly</option>
              <option value="quarter">Quarterly</option>
            </select>
          </label>
          <div className="compact-preset-summary">
            <strong>{query.timelinePlatforms.length ? `${query.timelinePlatforms.length} platform${query.timelinePlatforms.length === 1 ? "" : "s"} selected` : "No technology timing filter"}</strong>
            <span>{query.timelinePlatforms.length ? `${query.timelinePlatforms.map(humanizeToken).join(", ")} · ${techTimingSummary}` : "Choose one or more platforms to unlock the cohort chart and tech timing filters."}</span>
          </div>
        </div>
        <details
          className="timeline-details"
          open={
            hasTimelineSelection ||
            Boolean(query.timelineSeenFrom || query.timelineSeenTo || query.cmsMigrationFrom || query.cmsMigrationTo || query.domainMigrationFrom || query.domainMigrationTo)
          }
        >
          <summary>Timing filters</summary>
          <div className="timeline-details-grid">
            <section className="timeline-tech-picker compact-card">
              <div className="filter-header">
                <h3>Tech timing platforms</h3>
                <span className="muted">{query.timelinePlatforms.length} selected</span>
              </div>
              <div className="checklist compact-checklist">
                {(options?.timelinePlatforms ?? []).map((platform) => (
                  <label key={platform}>
                    <input
                      checked={query.timelinePlatforms.includes(platform)}
                      onChange={() => updateQuery({ timelinePlatforms: toggle(query.timelinePlatforms, platform) })}
                      type="checkbox"
                    />
                    <span>{humanizeToken(platform)}</span>
                  </label>
                ))}
              </div>
            </section>
            <section className="timeline-control-stack compact-card">
              <div className="filter-header">
                <h3>Tech timing</h3>
                <span className="muted">{techTimingSummary}</span>
              </div>
              <div className="timeline-date-row compact-date-row">
                <label className="field compact-field">
                  <span>Date basis</span>
                  <select
                    value={query.timelineDateField}
                    onChange={(event) => updateQuery({ timelineDateField: event.target.value as TimelineDateField })}
                  >
                    <option value="first_seen">First seen</option>
                    <option value="last_seen">Last seen</option>
                  </select>
                </label>
                <label className="field compact-field">
                  <span>Quick preset</span>
                  <select
                    value={techTimingPresetValue}
                    onChange={(event) => {
                      const value = event.target.value;
                      if (value === "all") handleTimelineQuickRange("all");
                      else if (value === "3") handleTimelineQuickRange(3);
                      else if (value === "6") handleTimelineQuickRange(6);
                      else if (value === "12") handleTimelineQuickRange(12);
                      else if (value === "24") handleTimelineQuickRange(24);
                    }}
                  >
                    <option value="all">All time</option>
                    <option value="3">Past 3 months</option>
                    <option value="6">Past 6 months</option>
                    <option value="12">Past 12 months</option>
                    <option value="24">Past 24 months</option>
                    <option value="custom">Custom dates below</option>
                  </select>
                </label>
              </div>
              <div className="timeline-event-group">
                <span className="group-label">Signals</span>
                <div className="checklist">
                  {(Object.keys(TIMELINE_EVENT_LABELS) as TimelineEventType[]).map((eventType) => (
                    <label key={eventType}>
                      <input checked={query.timelineEventTypes.includes(eventType)} onChange={() => toggleTimelineEventType(eventType)} type="checkbox" />
                      <span>{TIMELINE_EVENT_LABELS[eventType]}</span>
                    </label>
                  ))}
                </div>
              </div>
              <div className="timeline-date-row compact-date-row">
                <label className="field compact-field">
                  <span>{query.timelineDateField === "last_seen" ? "Last seen from" : "First seen from"}</span>
                  <input type="date" value={query.timelineSeenFrom} onChange={(event) => updateQuery({ timelineSeenFrom: event.target.value })} />
                </label>
                <label className="field compact-field">
                  <span>{query.timelineDateField === "last_seen" ? "Last seen to" : "First seen to"}</span>
                  <input type="date" value={query.timelineSeenTo} onChange={(event) => updateQuery({ timelineSeenTo: event.target.value })} />
                </label>
              </div>
            </section>
            <section className="timeline-control-stack compact-card">
              <div className="filter-header">
                <h3>Migration timing</h3>
                <span className="muted">{hasMigrationTiming ? migrationTimingLogicLabel : "Separate CMS and domain windows"}</span>
              </div>
              <label className="field compact-field">
                <span>Migration timing logic</span>
                <select
                  value={query.migrationTimingOperator}
                  onChange={(event) => updateQuery({ migrationTimingOperator: event.target.value as MigrationTimingOperator })}
                >
                  <option value="and">Match both windows</option>
                  <option value="or">Match either window</option>
                </select>
              </label>
              <div className="migration-date-groups">
                <div className="migration-date-group">
                  <div className="filter-header">
                    <h4>CMS migration</h4>
                    <span className="muted">{cmsTimingSummary}</span>
                  </div>
                  <label className="field compact-field">
                    <span>Quick preset</span>
                    <select
                      value={cmsTimingPresetValue}
                      onChange={(event) => {
                        const value = event.target.value;
                        if (value === "all") handleCmsMigrationQuickRange("all");
                        else if (value === "3") handleCmsMigrationQuickRange(3);
                        else if (value === "6") handleCmsMigrationQuickRange(6);
                        else if (value === "12") handleCmsMigrationQuickRange(12);
                        else if (value === "24") handleCmsMigrationQuickRange(24);
                      }}
                    >
                      <option value="all">All time</option>
                      <option value="3">Past 3 months</option>
                      <option value="6">Past 6 months</option>
                      <option value="12">Past 12 months</option>
                      <option value="24">Past 24 months</option>
                      <option value="custom">Custom dates below</option>
                    </select>
                  </label>
                  <div className="timeline-date-row compact-date-row">
                    <label className="field compact-field">
                      <span>From</span>
                      <input type="date" value={query.cmsMigrationFrom} onChange={(event) => updateQuery({ cmsMigrationFrom: event.target.value })} />
                    </label>
                    <label className="field compact-field">
                      <span>To</span>
                      <input type="date" value={query.cmsMigrationTo} onChange={(event) => updateQuery({ cmsMigrationTo: event.target.value })} />
                    </label>
                  </div>
                </div>
                <div className="migration-date-group">
                  <div className="filter-header">
                    <h4>Domain migration</h4>
                    <span className="muted">{domainTimingSummary}</span>
                  </div>
                  <label className="field compact-field">
                    <span>Quick preset</span>
                    <select
                      value={domainTimingPresetValue}
                      onChange={(event) => {
                        const value = event.target.value;
                        if (value === "all") handleDomainMigrationQuickRange("all");
                        else if (value === "3") handleDomainMigrationQuickRange(3);
                        else if (value === "6") handleDomainMigrationQuickRange(6);
                        else if (value === "12") handleDomainMigrationQuickRange(12);
                        else if (value === "24") handleDomainMigrationQuickRange(24);
                      }}
                    >
                      <option value="all">All time</option>
                      <option value="3">Past 3 months</option>
                      <option value="6">Past 6 months</option>
                      <option value="12">Past 12 months</option>
                      <option value="24">Past 24 months</option>
                      <option value="custom">Custom dates below</option>
                    </select>
                  </label>
                  <div className="timeline-date-row compact-date-row">
                    <label className="field compact-field">
                      <span>From</span>
                      <input type="date" value={query.domainMigrationFrom} onChange={(event) => updateQuery({ domainMigrationFrom: event.target.value })} />
                    </label>
                    <label className="field compact-field">
                      <span>To</span>
                      <input type="date" value={query.domainMigrationTo} onChange={(event) => updateQuery({ domainMigrationTo: event.target.value })} />
                    </label>
                  </div>
                </div>
              </div>
            </section>
          </div>
        </details>
        {hasTimelineSelection ? (
          <TimelineCardBody timeline={timeline} loading={timelineLoading} dateField={query.timelineDateField} />
        ) : (
          <div className="timeline-empty-state">
            Pick one or more platforms to unlock the cohort chart and tech timing timeline.
          </div>
        )}
      </section>

      <section className={`analytics-ribbon ${analyticsExpanded ? "analytics-ribbon-open" : ""}`}>
        <button className="analytics-summary-bar" onClick={() => setAnalyticsExpanded((current) => !current)} type="button">
          <div className="analytics-summary-title">
            <span className="kicker-inline">Dataset summary</span>
            <strong>{analyticsExpanded ? "Hide analytics" : "Show analytics"}</strong>
          </div>
          <div className="analytics-summary-metrics">
            <span><small>Filtered</small><strong>{analyticsLoading ? "…" : analytics?.kpis.filteredLeads ?? leads?.total ?? "…"}</strong></span>
            <span><small>Selected</small><strong>{tray?.count ?? "…"}</strong></span>
            <span><small>Priority A/B</small><strong>{analyticsLoading ? "…" : analytics?.kpis.priorityAB ?? "…"}</strong></span>
            <span><small>Recent migrations</small><strong>{analyticsLoading ? "…" : analytics?.kpis.recentMigrations ?? "…"}</strong></span>
          </div>
          <span className="analytics-summary-toggle">{analyticsExpanded ? "−" : "+"}</span>
        </button>
        {analyticsExpanded ? (
          <div className="analytics-ribbon-grid">
            <MetricCard label="Filtered leads" value={analyticsLoading ? "…" : analytics?.kpis.filteredLeads ?? leads?.total ?? "…"} />
            <MetricCard label="Selected leads" value={tray?.count ?? "…"} />
            <MetricCard label="Priority A/B" value={analyticsLoading ? "…" : analytics?.kpis.priorityAB ?? "…"} />
            <MetricCard label="Recent migrations" value={analyticsLoading ? "…" : analytics?.kpis.recentMigrations ?? "…"} />
            <MixCard title="Country mix" mode="stacked" data={analyticsLoading ? [] : analytics?.countryMix ?? []} />
            <MixCard title="Current platform mix" mode="bars" data={analyticsLoading ? [] : analytics?.currentPlatformMix ?? []} />
            <MixCard title="Lead angle mix" mode="bars" data={analyticsLoading ? [] : analytics?.salesBucketMix ?? []} />
            <MixCard title="Top migration corridors" mode="bars" data={analyticsLoading ? [] : analytics?.topCorridors ?? []} />
          </div>
        ) : null}
      </section>

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
                activeCount={searchSectionCount}
                open={sidebarSections.search}
                onToggle={() => toggleSidebarSection("search")}
              >
                <label className="field">
                  <span>Search or exact domain</span>
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
                      placeholder="domain, company, tool, vertical"
                    />
                    <button
                      className="ghost-button small-button"
                      disabled={searchDraft.trim() === query.search.trim()}
                      onClick={applySearchDraft}
                      type="button"
                    >
                      Search
                    </button>
                  </div>
                </label>
              </SidebarSection>

              <SidebarSection
                title="Commonly used"
                activeCount={commonSectionCount}
                open={sidebarSections.common}
                onToggle={() => toggleSidebarSection("common")}
              >
                <section className="filter-block">
                  <div className="filter-header">
                    <h3>Verticals</h3>
                    <span className="muted">{query.verticals.length} selected</span>
                  </div>
                  <label className="field compact-field">
                    <span>Search verticals</span>
                    <input
                      value={verticalSearch}
                      onChange={(event) => setVerticalSearch(event.target.value)}
                      placeholder="fashion, industrial, beauty"
                    />
                  </label>
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
                />

                <FilterBlock
                  title="Previous platform seen"
                  items={options?.removedPlatforms ?? []}
                  selected={query.removedPlatforms}
                  onToggle={(value) => updateQuery({ removedPlatforms: toggle(query.removedPlatforms, value) })}
                  formatLabel={humanizeToken}
                />

                <FilterBlock
                  title="New platform seen"
                  items={options?.recentPlatforms ?? []}
                  selected={query.recentPlatforms}
                  onToggle={(value) => updateQuery({ recentPlatforms: toggle(query.recentPlatforms, value) })}
                  formatLabel={humanizeToken}
                />

                <FilterBlock
                  title="Priority tiers"
                  items={options?.tiers ?? ["A", "B", "C", "D"]}
                  selected={query.tiers}
                  onToggle={(value) => updateQuery({ tiers: toggle(query.tiers, value) })}
                />

                <div className="toggle-grid compact-toggle-grid">
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
                    label="Has SE Ranking analysis"
                    checked={query.hasSeRankingAnalysis}
                    onChange={() => updateQuery({ hasSeRankingAnalysis: !query.hasSeRankingAnalysis })}
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
              </SidebarSection>

              <SidebarSection
                title="Advanced"
                activeCount={advancedSectionCount}
                open={sidebarSections.advanced}
                onToggle={() => toggleSidebarSection("advanced")}
              >
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
              </SidebarSection>
            </>
          )}
        </aside>

        <section className="grid-panel">
          <div className="grid-toolbar">
            <div>
              <h2>Lead worksheet</h2>
              <p>
                {loading ? "Refreshing…" : `${leads?.total ?? 0} matching leads`} · sorted by {sortBadge(query)}
              </p>
            </div>
            <div className="toolbar-actions">
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

          <div className="active-filter-row">
            {activeFilterChips.map((chip) => (
              <button className="filter-chip" key={`${chip.group}-${chip.label}`} onClick={chip.clear} type="button">
                <span>{chip.group}</span>
                <strong>{chip.label}</strong>
                <small>×</small>
              </button>
            ))}
          </div>

          <div className="quick-toggle-row">
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
          </div>

          <section className={`migration-timing-row ${hasMigrationTiming ? "migration-timing-active" : ""}`}>
            <div className="migration-timing-summary">
              <span className="kicker-inline">Migration timing</span>
              <strong>{hasMigrationTiming ? migrationTimingLogicLabel : "No migration timing filter"}</strong>
            </div>
            <div className="migration-timing-controls">
              <button
                className={`migration-timing-chip ${hasCmsTiming ? "migration-timing-chip-active" : ""}`}
                onClick={() => {
                  if (hasCmsTiming) {
                    clearCmsMigrationTiming();
                  }
                }}
                type="button"
              >
                <span>CMS migration</span>
                <strong>{cmsTimingSummary}</strong>
              </button>
              <div className="migration-timing-logic" role="group" aria-label="Migration timing logic">
                <button
                  className={`ghost-button small-button ${query.migrationTimingOperator === "and" ? "timeline-toggle-active" : ""}`}
                  onClick={() => updateQuery({ migrationTimingOperator: "and" })}
                  type="button"
                >
                  AND
                </button>
                <button
                  className={`ghost-button small-button ${query.migrationTimingOperator === "or" ? "timeline-toggle-active" : ""}`}
                  onClick={() => updateQuery({ migrationTimingOperator: "or" })}
                  type="button"
                >
                  OR
                </button>
              </div>
              <button
                className={`migration-timing-chip ${hasDomainTiming ? "migration-timing-chip-active" : ""}`}
                onClick={() => {
                  if (hasDomainTiming) {
                    clearDomainMigrationTiming();
                  }
                }}
                type="button"
              >
                <span>Domain migration</span>
                <strong>{domainTimingSummary}</strong>
              </button>
            </div>
            <div className="migration-timing-actions">
              <button className="ghost-button small-button" disabled={!hasCmsTiming} onClick={clearCmsMigrationTiming} type="button">
                Clear CMS
              </button>
              <button className="ghost-button small-button" disabled={!hasDomainTiming} onClick={clearDomainMigrationTiming} type="button">
                Clear domain
              </button>
              <button className="ghost-button small-button" disabled={!hasMigrationTiming} onClick={clearAllMigrationTiming} type="button">
                Clear all
              </button>
            </div>
          </section>

          <section className="se-ranking-bar">
            <div className="se-ranking-summary">
              <span className="kicker-inline">SE Ranking analysis</span>
              <strong>{tray?.count ?? 0} selected leads</strong>
              <small>
                {seRankingSummary
                  ? `${seRankingSummary.summary.eligibleCount} eligible · ${seRankingSummary.summary.estimatedCredits} credits`
                  : "Select tray leads to estimate cost"}
              </small>
            </div>
            <div className="se-ranking-controls">
              <label className="field compact-field se-ranking-field">
                <span>Migration type</span>
                <select value={seRankingType} onChange={(event) => setSeRankingType(event.target.value as SeRankingAnalysisType)}>
                  <option value="cms_migration">CMS migration</option>
                  <option value="domain_migration">Domain migration</option>
                </select>
              </label>
              <div className="se-ranking-estimate">
                <span>To run {seRankingSummary?.summary.toRunCount ?? 0}</span>
                <span>Already analyzed {seRankingSummary?.summary.alreadyAnalyzedCount ?? 0}</span>
              </div>
            </div>
            <div className="se-ranking-actions">
              <button className="ghost-button small-button" disabled={!tray?.count || seRankingLoading} onClick={() => void handleRunSeRankingAnalysis(false)} type="button">
                Refresh estimate
              </button>
              <button
                className="primary-button small-button"
                disabled={!seRankingSummary?.summary.toRunCount || seRankingLoading}
                onClick={() => void handleRunSeRankingAnalysis(true)}
                type="button"
              >
                {seRankingLoading ? "Running…" : "Confirm and run"}
              </button>
              <button
                className="ghost-button small-button"
                disabled={!seRankingSummary?.summary.alreadyAnalyzedCount || seRankingLoading}
                onClick={() => void handleRefreshSeRankingAnalysis()}
                type="button"
              >
                Refresh selected results
              </button>
            </div>
          </section>

          {error ? <div className="error-box">{error}</div> : null}

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
                      <td className="sticky sticky-domain mono-cell">{lead.root_domain}</td>
                      {effectiveVisibleColumns.map((column) => (
                        <td key={column}>{renderCell(column, lead, query.salesBuckets)}</td>
                      ))}
                    </tr>
                  );
                })}
              </tbody>
            </table>

            {!loading && !leads?.items.length ? <div className="empty-state">No leads match the current filters.</div> : null}
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
              <div className="drawer-meta">
                <span>{detail.lead.root_domain}</span>
                <span>{humanizeToken(detail.lead.country)}</span>
                <span>Tier {detail.lead.priority_tier}</span>
                <span>Score {detail.lead.total_score}</span>
              </div>
              <div className="evidence-strip">
                <StatusBadge label={evidenceQualityLabel(detail.lead)} tone={confidenceTone(detail.lead.domain_migration_status || detail.lead.cms_migration_status)} />
                {detail.lead.domain_migration_status !== "none" ? (
                  <StatusBadge label={`Previous domain: ${humanizeToken(detail.lead.domain_migration_status)}`} tone={confidenceTone(detail.lead.domain_migration_status)} />
                ) : null}
                {detail.lead.cms_migration_status !== "none" ? (
                  <StatusBadge label={`CMS: ${humanizeToken(detail.lead.cms_migration_status)}`} tone={confidenceTone(detail.lead.cms_migration_status)} />
                ) : null}
              </div>
              {detail.data_quality.notes.length ? (
                <div className="pill-row">
                  {detail.data_quality.notes.map((note) => (
                    <span className="pill signal-pill" key={note}>
                      {note}
                    </span>
                  ))}
                </div>
              ) : null}
              <div className="drawer-nav">
                <button onClick={() => navigateDrawer("prev")} type="button">
                  Previous
                </button>
                <button onClick={() => navigateDrawer("next")} type="button">
                  Next
                </button>
              </div>
            </section>

            <section className="drawer-section">
              <h3>Migration intelligence</h3>
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

              <div className="migration-detail-grid">
                <article className="migration-detail-panel">
                  <div className="migration-detail-header">
                    <h4>Why we think this is a domain migration</h4>
                    <span>{detail.migrationIntelligence.summary.domainCandidateCount} candidates</span>
                  </div>
                  {detail.migrationIntelligence.domainMigration.bestMatch ? (
                    <>
                      <SignalChipRow signals={detail.lead.domain_migration_warning_flags} emptyText="No domain-migration warnings for this lead." />
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
                        <ComparisonTile
                          label="Old platform"
                          value={detail.migrationIntelligence.domainMigration.bestMatch.old_ecommerce_platforms || "Unknown"}
                        />
                        <ComparisonTile
                          label="Evidence"
                          value={detail.migrationIntelligence.domainMigration.bestMatch.fingerprint_strength || "Unknown"}
                        />
                      </div>
                      <p className="migration-copy">{detail.migrationIntelligence.domainMigration.bestMatch.notes || "No summary note available."}</p>
                      {detail.migrationIntelligence.domainMigration.bestMatch.fingerprint_notes ? (
                        <p className="migration-copy subtle-copy">
                          {detail.migrationIntelligence.domainMigration.bestMatch.fingerprint_notes}
                        </p>
                      ) : null}
                      <DomainCandidateList candidates={detail.migrationIntelligence.domainMigration.candidateShortlist} />
                    </>
                  ) : (
                    <p className="muted">No previous-domain evidence available for this lead.</p>
                  )}
                </article>

                <article className="migration-detail-panel">
                  <div className="migration-detail-header">
                    <h4>Why we think this is a CMS migration</h4>
                    <span>{detail.migrationIntelligence.summary.cmsCandidateCount} candidates</span>
                  </div>
                  {detail.migrationIntelligence.cmsMigration.candidatePairs.length ? (
                    <>
                      <SignalChipRow signals={detail.lead.cms_migration_warning_flags} emptyText="No CMS migration warnings for this lead." />
                      <TechnologyChipRow label="Evidence" values={detail.lead.cms_migration_evidence_flags} />
                      <MigrationList migrations={detail.migrationIntelligence.cmsMigration.candidatePairs.slice(0, 5)} />
                      <DrawerTimelineChart rows={detail.timelineRows} />
                      <ul className="event-list compact-list">
                        {detail.events.slice(0, 10).map((event, index) => (
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
                    </>
                  ) : (
                    <>
                      <p className="muted">No CMS migration evidence available for this lead.</p>
                      <DrawerTimelineChart rows={detail.timelineRows} />
                    </>
                  )}
                </article>
              </div>
            </section>

          <section className="drawer-section">
            <h3>Bucket evidence</h3>
            <div className="pill-row">{pillList(detail.lead.sales_buckets, 10)}</div>
            <ul className="reason-list">
              {detail.exportReady.bucket_reasons.map((reason) => (
                  <li key={reason}>{humanizeReason(reason)}</li>
              ))}
            </ul>
          </section>

            <section className="drawer-section">
              <h3>Contacts</h3>
              <InfoList title="Emails" items={detail.lead.emails} />
              <InfoList title="Phones" items={detail.lead.telephones} />
              <InfoList title="People" items={detail.lead.people} />
              <InfoList title="Verified" items={detail.lead.verified_profiles} />
            </section>

            <section className="drawer-section">
              <h3>Stack</h3>
              <DrawerPillGroup title="Current platform" items={detail.lead.current_platforms.length ? detail.lead.current_platforms : detail.lead.current_candidate_platforms} />
              <DrawerPillGroup title="Previous platform seen" items={detail.lead.removed_platforms} />
              <DrawerPillGroup title="Marketing" items={detail.lead.marketing_platforms} />
              <DrawerPillGroup title="CRM" items={detail.lead.crm_platforms} />
              <DrawerPillGroup title="Payments" items={detail.lead.payment_platforms} />
              <DrawerPillGroup title="Hosting" items={detail.lead.hosting_providers} />
            </section>

            <section className="drawer-section">
              <h3>SE Ranking outcome</h3>
              {detail.seRankingAnalysis ? (
                <div className="se-drawer-grid">
                  <ComparisonTile label="Analysis type" value={humanizeToken(String(detail.seRankingAnalysis.analysis_type || "unknown"))} />
                  <ComparisonTile label="Market" value={String(detail.seRankingAnalysis.regional_source || "—").toUpperCase()} />
                  <ComparisonTile label="Migration date" value={formatDate(String(detail.seRankingAnalysis.migration_likely_date || ""))} />
                  <ComparisonTile label="Checked" value={formatDate(String(detail.seRankingAnalysis.captured_at || ""))} />
                  <ComparisonTile label="Traffic before" value={formatNumber(Number(detail.seRankingAnalysis.traffic_before || 0))} />
                  <ComparisonTile label="Traffic last month" value={formatNumber(Number(detail.seRankingAnalysis.traffic_last_month || 0))} />
                  <ComparisonTile label="Keyword count before" value={formatNumber(Number(detail.seRankingAnalysis.keywords_before || 0))} />
                  <ComparisonTile label="Keyword count last month" value={formatNumber(Number(detail.seRankingAnalysis.keywords_last_month || 0))} />
                </div>
              ) : (
                <p className="muted">No SE Ranking analysis saved for this lead yet.</p>
              )}
              {detail.seRankingAnalysis ? (
                <>
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
                </>
              ) : null}
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

      {toast ? <div className="toast">{toast}</div> : null}
    </div>
  );
}

function MetricCard(props: { label: string; value: number | string }) {
  return (
    <article className="metric-card">
      <span>{props.label}</span>
      <strong>{props.value}</strong>
    </article>
  );
}

function MixCard(props: { title: string; data: Array<{ label: string; count: number }>; mode: "stacked" | "bars" }) {
  const total = props.data.reduce((sum, item) => sum + item.count, 0);
  return (
    <article className="mix-card">
      <div className="mix-header">
        <h3>{props.title}</h3>
        <span>{total.toLocaleString()}</span>
      </div>
      {props.mode === "stacked" ? (
        <div className="stacked-bar">
          {props.data.map((item) => (
            <span
              key={item.label}
              style={{ width: total ? `${(item.count / total) * 100}%` : "0%" }}
              title={`${item.label}: ${item.count.toLocaleString()}`}
            />
          ))}
        </div>
      ) : null}
      <ul className="mix-list">
        {props.data.map((item) => (
          <li key={item.label}>
            <div>
              <strong>{humanizeToken(item.label)}</strong>
              <small>{item.count.toLocaleString()}</small>
            </div>
            <div className="bar-track">
              <span style={{ width: total ? `${(item.count / total) * 100}%` : "0%" }} />
            </div>
          </li>
        ))}
      </ul>
    </article>
  );
}

function TimelineCardBody(props: { timeline: TimelineCohortResponse | null; loading: boolean; dateField: TimelineDateField }) {
  if (props.loading) {
    return <div className="timeline-empty-state">Loading timeline…</div>;
  }
  if (!props.timeline || !props.timeline.series.length) {
    return <div className="timeline-empty-state">No technology dates match the current tech timing filters.</div>;
  }
  const timeline = props.timeline;
  const countLabel = props.dateField === "last_seen" ? "Last seen" : "Starts";
  const rangeLabel = props.dateField === "last_seen" ? "Available last-seen range" : "Available first-seen range";

  return (
    <div className="timeline-results">
      <div className="timeline-kpis">
        <MetricCard label={countLabel} value={timeline.summary.totalStarts.toLocaleString()} />
        <MetricCard label="Domains" value={timeline.summary.uniqueDomains.toLocaleString()} />
        <MetricCard label="Periods" value={timeline.summary.periodCount.toLocaleString()} />
        <MetricCard
          label="Range"
          value={
            timeline.summary.firstPeriod && timeline.summary.lastPeriod
              ? `${timeline.summary.firstPeriod} → ${timeline.summary.lastPeriod}`
              : "—"
          }
        />
      </div>
      <div className="timeline-chart-shell">
        <TimelineLineChart timeline={timeline} />
        <div className="timeline-breakdown">
          <h3>{props.dateField === "last_seen" ? "Last-seen breakdown" : "CMS breakdown"}</h3>
          <ul className="mix-list">
            {timeline.technologyBreakdown.map((item) => (
              <li key={item.platform}>
                <div>
                  <strong>{humanizeToken(item.platform)}</strong>
                  <small>{item.count.toLocaleString()}</small>
                </div>
                <div className="bar-track">
                  <span
                    style={{
                      width: timeline.summary.totalStarts
                        ? `${(item.count / timeline.summary.totalStarts) * 100}%`
                        : "0%",
                      background: colourForToken(item.platform),
                    }}
                  />
                </div>
              </li>
            ))}
          </ul>
          <p className="muted">
            {rangeLabel}: {timeline.availableRange.minDate ?? "—"} to {timeline.availableRange.maxDate ?? "—"}
          </p>
        </div>
      </div>
    </div>
  );
}

function TimelineLineChart(props: { timeline: TimelineCohortResponse }) {
  const periods = props.timeline.series.map((item) => item.period);
  const platformSeries = props.timeline.seriesByPlatform.filter((series) => series.points.some((point) => point.count > 0));
  const maxCount = Math.max(
    1,
    ...props.timeline.series.map((item) => item.count),
    ...platformSeries.flatMap((series) => series.points.map((point) => point.count)),
  );
  const width = Math.max(periods.length * 28, 360);
  const height = 170;
  const plotLeft = 14;
  const plotRight = 14;
  const plotTop = 12;
  const plotBottom = 128;
  const chartWidth = Math.max(width - plotLeft - plotRight, 1);
  const chartHeight = plotBottom - plotTop;
  const xForIndex = (index: number) =>
    periods.length === 1 ? plotLeft + chartWidth / 2 : plotLeft + (index / Math.max(periods.length - 1, 1)) * chartWidth;
  const yForCount = (count: number) => plotBottom - (count / maxCount) * chartHeight;

  return (
    <div className="timeline-chart timeline-line-chart">
      <svg viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none" role="img" aria-label="Cohort timeline">
        {[0.25, 0.5, 0.75].map((ratio) => {
          const y = plotBottom - chartHeight * ratio;
          return <line className="timeline-grid-line" key={ratio} x1={plotLeft} x2={width - plotRight} y1={y} y2={y} />;
        })}
        {props.timeline.series.map((item, index) => {
          const barWidth = Math.max(chartWidth / Math.max(props.timeline.series.length, 1) - 8, 3);
          const x = xForIndex(index) - barWidth / 2;
          const barHeight = Math.max((item.count / maxCount) * chartHeight, 2);
          const y = plotBottom - barHeight;
          return <rect className="timeline-bar timeline-bar-total" key={item.period} x={x} y={y} width={barWidth} height={barHeight} rx={3} ry={3} />;
        })}
        {platformSeries.map((series) => {
          const colour = colourForToken(series.platform);
          const path = series.points
            .map((point, index) => `${index === 0 ? "M" : "L"} ${xForIndex(index)} ${yForCount(point.count)}`)
            .join(" ");
          return (
            <g key={series.platform}>
              <path d={path} fill="none" stroke={colour} strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" />
              {series.points.map((point, index) => (
                <circle
                  key={`${series.platform}-${point.period}`}
                  cx={xForIndex(index)}
                  cy={yForCount(point.count)}
                  fill={colour}
                  r={point.count ? 2.8 : 1.6}
                />
              ))}
            </g>
          );
        })}
      </svg>
      <div className="timeline-axis">
        <span>{periods[0]}</span>
        <span>{periods.at(-1)}</span>
      </div>
      <div className="timeline-legend">
        {platformSeries.map((series) => (
          <span className="timeline-legend-item" key={series.platform}>
            <i style={{ background: colourForToken(series.platform) }} />
            {humanizeToken(series.platform)}
          </span>
        ))}
      </div>
    </div>
  );
}

function SidebarSection(props: {
  title: string;
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
      {props.open ? <div className="sidebar-section-body">{props.children}</div> : null}
    </section>
  );
}

function FilterBlock(props: {
  title: string;
  items: string[];
  selected: string[];
  onToggle: (value: string) => void;
  formatLabel?: (value: string) => string;
}) {
  return (
    <section className="filter-block">
      {props.title ? <h3>{props.title}</h3> : null}
      <div className="checklist">
        {props.items.map((item) => (
          <label key={item}>
            <input checked={props.selected.includes(item)} onChange={() => props.onToggle(item)} type="checkbox" />
            <span>{props.formatLabel ? props.formatLabel(item) : item}</span>
          </label>
        ))}
      </div>
    </section>
  );
}

function ToggleRow(props: { label: string; checked: boolean; onChange: () => void }) {
  return (
    <label className="toggle-row">
      <span>{props.label}</span>
      <input checked={props.checked} onChange={props.onChange} type="checkbox" />
    </label>
  );
}

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

function InfoList(props: { title: string; items: string[] }) {
  return (
    <div className="info-list">
      <span>{props.title}</span>
      {props.items.length ? (
        <ul>
          {props.items.map((item) => (
            <li key={item}>{item}</li>
          ))}
        </ul>
      ) : (
        <p className="muted">None</p>
      )}
    </div>
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

function renderCell(column: ColumnKey, lead: Lead, selectedBuckets: string[]) {
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
          <small>{lead.se_ranking_baseline_month || "No baseline month"}</small>
        </div>
      );
    case "se_traffic_last_month":
      return (
        <div className="tight-cell">
          <strong>{formatNumber(lead.se_ranking_traffic_last_month)}</strong>
          <small>{lead.se_ranking_comparison_month || "No comparison month"}</small>
        </div>
      );
    case "se_traffic_change":
      return lead.se_ranking_status ? (
        <div className="tight-cell">
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
        <div className="tight-cell">
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
          <small>{lead.se_ranking_error_message || humanizeToken(lead.se_ranking_analysis_type || "history only")}</small>
        </div>
      ) : (
        <span className="muted">Not checked</span>
      );
    case "se_checked":
      return (
        <div className="tight-cell">
          <strong>{formatDate(lead.se_ranking_checked_at)}</strong>
          <small>{humanizeToken(lead.se_ranking_analysis_type || "not_checked")}</small>
        </div>
      );
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
    case "reason":
      return <span className="reason-cell">{matchingReasonText(lead, selectedBuckets)}</span>;
    default:
      return null;
  }
}
