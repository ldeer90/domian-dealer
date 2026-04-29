import type {
  AnalyticsResponse,
  ExportTrayResponse,
  FilterOptions,
  HealthResponse,
  LeadDetailResponse,
  LeadQuery,
  LeadsResponse,
  Preset,
  PresetsResponse,
  ScreamingFrogRunResponse,
  ScreamingFrogJobBatch,
  ScreamingFrogSummaryResponse,
  SeRankingManualPreviewResponse,
  SeRankingManualRequest,
  SeRankingManualRunResponse,
  SeRankingRunResponse,
  SeRankingSummaryResponse,
  SiteStatusRunResponse,
  SiteStatusSummaryResponse,
  SummaryResponse,
  TimelineCohortResponse,
} from "./types";

const API_BASE = (import.meta.env.VITE_API_BASE as string | undefined) ?? "http://127.0.0.1:8765/api";

const SERIALIZED_QUERY_KEYS = [
  "search",
  "exactDomain",
  "countries",
  "tiers",
  "currentPlatforms",
  "recentPlatforms",
  "removedPlatforms",
  "verticals",
  "salesBuckets",
  "liveSitesOnly",
  "migrationOnly",
  "hasDomainMigration",
  "hasCmsMigration",
  "domainMigrationStatuses",
  "domainConfidenceBands",
  "domainFingerprintStrengths",
  "domainTldRelationships",
  "cmsMigrationStatuses",
  "cmsConfidenceLevels",
  "hasContact",
  "hasMarketing",
  "hasCrm",
  "hasPayments",
  "marketingPlatforms",
  "crmPlatforms",
  "paymentPlatforms",
  "hostingProviders",
  "agencies",
  "aiTools",
  "complianceFlags",
  "minSocial",
  "minRevenue",
  "minEmployees",
  "minSku",
  "minTechnologySpend",
  "selectedOnly",
  "hasSeRankingAnalysis",
  "seRankingAnalysisTypes",
  "seRankingOutcomeFlags",
  "hasSiteStatusCheck",
  "siteStatusCategories",
  "hasScreamingFrogAudit",
  "screamingFrogStatuses",
  "screamingFrogHomepageStatuses",
  "screamingFrogTitleFlags",
  "screamingFrogMetaFlags",
  "screamingFrogCanonicalFlags",
  "hasScreamingFrogInternalErrors",
  "hasScreamingFrogLocationPages",
  "hasScreamingFrogServicePages",
  "timelinePlatforms",
  "timelineEventTypes",
  "timelineDateField",
  "timelineSeenFrom",
  "timelineSeenTo",
  "cmsMigrationFrom",
  "cmsMigrationTo",
  "cmsUnchangedYears",
  "domainMigrationFrom",
  "domainMigrationTo",
  "migrationTimingOperator",
  "timelineGranularity",
  "page",
  "pageSize",
  "sortBy",
  "sortDirection",
] as const satisfies readonly (keyof LeadQuery)[];

type MissingSerializedQueryKeys = Exclude<keyof LeadQuery, (typeof SERIALIZED_QUERY_KEYS)[number]>;
const assertSerializedQueryKeys: MissingSerializedQueryKeys extends never ? true : never = true;
void assertSerializedQueryKeys;

function buildQuery(params: Record<string, string | number | boolean | string[]>) {
  const searchParams = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (Array.isArray(value)) {
      value.filter(Boolean).forEach((item) => searchParams.append(key, item));
      return;
    }
    if (typeof value === "boolean") {
      if (value) {
        searchParams.set(key, "true");
      }
      return;
    }
    if (`${value}`.length > 0) {
      searchParams.set(key, `${value}`);
    }
  });
  return searchParams.toString();
}

type FetchJsonOptions = {
  retries?: number;
  retryDelayMs?: number;
  timeoutMs?: number;
};

async function fetchJson<T>(path: string, init?: RequestInit, options?: FetchJsonOptions): Promise<T> {
  const method = (init?.method ?? "GET").toUpperCase();
  const retries = options?.retries ?? (method === "GET" ? 1 : 0);
  const retryDelayMs = options?.retryDelayMs ?? 450;
  const timeoutMs = options?.timeoutMs ?? 12000;
  let lastError: Error | null = null;

  for (let attempt = 0; attempt <= retries; attempt += 1) {
    const controller = new AbortController();
    const timeoutHandle = window.setTimeout(() => controller.abort(), timeoutMs);
    try {
      const response = await fetch(`${API_BASE}${path}`, {
        headers: {
          "Content-Type": "application/json",
        },
        ...init,
        signal: controller.signal,
      });
      if (!response.ok) {
        throw new Error(`Request failed: ${response.status}`);
      }
      return (await response.json()) as T;
    } catch (error) {
      lastError =
        error instanceof DOMException && error.name === "AbortError" ? new Error("Request timed out") : (error as Error);
      if (attempt >= retries) {
        break;
      }
      await new Promise((resolve) => window.setTimeout(resolve, retryDelayMs * (attempt + 1)));
    } finally {
      window.clearTimeout(timeoutHandle);
    }
  }

  throw lastError ?? new Error("Request failed");
}

function queryParams(query: LeadQuery) {
  return buildQuery({
    search: query.search,
    exact_domain: query.exactDomain,
    countries: query.countries,
    tiers: query.tiers,
    current_platforms: query.currentPlatforms,
    recent_platforms: query.recentPlatforms,
    removed_platforms: query.removedPlatforms,
    verticals: query.verticals,
    sales_buckets: query.salesBuckets,
    live_sites_only: query.liveSitesOnly,
    timeline_platforms: query.timelinePlatforms,
    timeline_event_types: query.timelineEventTypes,
    timeline_date_field: query.timelineDateField,
    timeline_seen_from: query.timelineSeenFrom,
    timeline_seen_to: query.timelineSeenTo,
    cms_migration_from: query.cmsMigrationFrom,
    cms_migration_to: query.cmsMigrationTo,
    cms_unchanged_years: query.cmsUnchangedYears,
    domain_migration_from: query.domainMigrationFrom,
    domain_migration_to: query.domainMigrationTo,
    migration_timing_operator: query.migrationTimingOperator,
    migration_only: query.migrationOnly,
    has_domain_migration: query.hasDomainMigration,
    has_cms_migration: query.hasCmsMigration,
    domain_migration_statuses: query.domainMigrationStatuses,
    domain_confidence_bands: query.domainConfidenceBands,
    domain_fingerprint_strengths: query.domainFingerprintStrengths,
    domain_tld_relationships: query.domainTldRelationships,
    cms_migration_statuses: query.cmsMigrationStatuses,
    cms_confidence_levels: query.cmsConfidenceLevels,
    has_contact: query.hasContact,
    has_marketing: query.hasMarketing,
    has_crm: query.hasCrm,
    has_payments: query.hasPayments,
    marketing_platforms: query.marketingPlatforms,
    crm_platforms: query.crmPlatforms,
    payment_platforms: query.paymentPlatforms,
    hosting_providers: query.hostingProviders,
    agencies: query.agencies,
    ai_tools: query.aiTools,
    compliance_flags: query.complianceFlags,
    min_social: query.minSocial,
    min_revenue: query.minRevenue,
    min_employees: query.minEmployees,
    min_sku: query.minSku,
    min_technology_spend: query.minTechnologySpend,
    selected_only: query.selectedOnly,
    has_seranking_analysis: query.hasSeRankingAnalysis,
    seranking_analysis_types: query.seRankingAnalysisTypes,
    seranking_outcome_flags: query.seRankingOutcomeFlags,
    has_site_status_check: query.hasSiteStatusCheck,
    site_status_categories: query.siteStatusCategories,
    has_screamingfrog_audit: query.hasScreamingFrogAudit,
    screamingfrog_statuses: query.screamingFrogStatuses,
    screamingfrog_homepage_statuses: query.screamingFrogHomepageStatuses,
    screamingfrog_title_flags: query.screamingFrogTitleFlags,
    screamingfrog_meta_flags: query.screamingFrogMetaFlags,
    screamingfrog_canonical_flags: query.screamingFrogCanonicalFlags,
    has_screamingfrog_internal_errors: query.hasScreamingFrogInternalErrors,
    has_screamingfrog_location_pages: query.hasScreamingFrogLocationPages,
    has_screamingfrog_service_pages: query.hasScreamingFrogServicePages,
    page: query.page,
    page_size: query.pageSize,
    sort_by: query.sortBy,
    sort_direction: query.sortDirection,
  });
}

export function fetchSummary() {
  return fetchJson<SummaryResponse>("/summary");
}

export function fetchHealth() {
  return fetchJson<HealthResponse>("/health", undefined, { retries: 2, retryDelayMs: 500, timeoutMs: 5000 });
}

export function fetchFilterOptions(query?: LeadQuery) {
  const suffix = query ? `?${queryParams({ ...query, page: 1 })}` : "";
  return fetchJson<FilterOptions>(`/filter-options${suffix}`);
}

export function fetchAnalytics(query: LeadQuery) {
  return fetchJson<AnalyticsResponse>(`/analytics?${queryParams({ ...query, page: 1 })}`);
}

export function fetchTimelineCohort(query: LeadQuery) {
  const params = buildQuery({
    search: query.search,
    exact_domain: query.exactDomain,
    countries: query.countries,
    tiers: query.tiers,
    current_platforms: query.currentPlatforms,
    recent_platforms: query.recentPlatforms,
    removed_platforms: query.removedPlatforms,
    verticals: query.verticals,
    sales_buckets: query.salesBuckets,
    live_sites_only: query.liveSitesOnly,
    timeline_platforms: query.timelinePlatforms,
    timeline_event_types: query.timelineEventTypes,
    timeline_date_field: query.timelineDateField,
    timeline_seen_from: query.timelineSeenFrom,
    timeline_seen_to: query.timelineSeenTo,
    cms_migration_from: query.cmsMigrationFrom,
    cms_migration_to: query.cmsMigrationTo,
    domain_migration_from: query.domainMigrationFrom,
    domain_migration_to: query.domainMigrationTo,
    migration_timing_operator: query.migrationTimingOperator,
    granularity: query.timelineGranularity,
    migration_only: query.migrationOnly,
    has_domain_migration: query.hasDomainMigration,
    has_cms_migration: query.hasCmsMigration,
    domain_migration_statuses: query.domainMigrationStatuses,
    domain_confidence_bands: query.domainConfidenceBands,
    domain_fingerprint_strengths: query.domainFingerprintStrengths,
    domain_tld_relationships: query.domainTldRelationships,
    cms_migration_statuses: query.cmsMigrationStatuses,
    cms_confidence_levels: query.cmsConfidenceLevels,
    has_contact: query.hasContact,
    has_marketing: query.hasMarketing,
    has_crm: query.hasCrm,
    has_payments: query.hasPayments,
    marketing_platforms: query.marketingPlatforms,
    crm_platforms: query.crmPlatforms,
    payment_platforms: query.paymentPlatforms,
    hosting_providers: query.hostingProviders,
    agencies: query.agencies,
    ai_tools: query.aiTools,
    compliance_flags: query.complianceFlags,
    min_social: query.minSocial,
    min_revenue: query.minRevenue,
    min_employees: query.minEmployees,
    min_sku: query.minSku,
    min_technology_spend: query.minTechnologySpend,
    selected_only: query.selectedOnly,
    has_seranking_analysis: query.hasSeRankingAnalysis,
    seranking_analysis_types: query.seRankingAnalysisTypes,
    seranking_outcome_flags: query.seRankingOutcomeFlags,
    has_site_status_check: query.hasSiteStatusCheck,
    site_status_categories: query.siteStatusCategories,
    has_screamingfrog_audit: query.hasScreamingFrogAudit,
    screamingfrog_statuses: query.screamingFrogStatuses,
    screamingfrog_homepage_statuses: query.screamingFrogHomepageStatuses,
    screamingfrog_title_flags: query.screamingFrogTitleFlags,
    screamingfrog_meta_flags: query.screamingFrogMetaFlags,
    screamingfrog_canonical_flags: query.screamingFrogCanonicalFlags,
    has_screamingfrog_internal_errors: query.hasScreamingFrogInternalErrors,
    has_screamingfrog_location_pages: query.hasScreamingFrogLocationPages,
    has_screamingfrog_service_pages: query.hasScreamingFrogServicePages,
  });
  return fetchJson<TimelineCohortResponse>(`/timeline/cohort?${params}`);
}

export function fetchLeads(query: LeadQuery) {
  return fetchJson<LeadsResponse>(`/leads?${queryParams(query)}`);
}

export function fetchLeadDetail(rootDomain: string) {
  return fetchJson<LeadDetailResponse>(`/leads/${rootDomain}`);
}

export function exportLeadUrl(query: LeadQuery) {
  return `${API_BASE}/leads/export?${queryParams({ ...query, page: 1 })}`;
}

export function fetchPresets() {
  return fetchJson<PresetsResponse>("/presets");
}

export function createPreset(payload: { name: string; filters: LeadQuery; visibleColumns: string[]; sort: Preset["sort"] }) {
  return fetchJson<Preset>("/presets", {
    method: "POST",
    body: JSON.stringify({
      name: payload.name,
      filters: payload.filters,
      visible_columns: payload.visibleColumns,
      sort: payload.sort,
    }),
  });
}

export function updatePreset(presetId: string, payload: { name: string; filters: LeadQuery; visibleColumns: string[]; sort: Preset["sort"] }) {
  return fetchJson<Preset>(`/presets/${presetId}`, {
    method: "PUT",
    body: JSON.stringify({
      name: payload.name,
      filters: payload.filters,
      visible_columns: payload.visibleColumns,
      sort: payload.sort,
    }),
  });
}

export function deletePreset(presetId: string) {
  return fetchJson<{ ok: boolean }>(`/presets/${presetId}`, { method: "DELETE" });
}

export function fetchExportTray() {
  return fetchJson<ExportTrayResponse>("/export-tray");
}

export function addToExportTray(rootDomains: string[]) {
  return fetchJson<ExportTrayResponse>("/export-tray/items", {
    method: "POST",
    body: JSON.stringify({ root_domains: rootDomains }),
  });
}

export function removeFromExportTray(rootDomain: string) {
  return fetchJson<ExportTrayResponse>(`/export-tray/items/${rootDomain}`, {
    method: "DELETE",
  });
}

export function clearExportTray() {
  return fetchJson<ExportTrayResponse>("/export-tray/clear", {
    method: "POST",
    body: JSON.stringify({}),
  });
}

export function addFilteredToExportTray(query: LeadQuery) {
  return fetchJson<ExportTrayResponse>(`/export-tray/select-filtered?${queryParams({ ...query, page: 1 })}`, {
    method: "POST",
    body: JSON.stringify({}),
  });
}

export function fetchSeRankingSummary(
  analysisType: "cms_migration" | "domain_migration",
  query?: LeadQuery,
  useFilteredView = false,
) {
  const params = query
    ? `${buildQuery({ analysis_type: analysisType, use_filtered_view: useFilteredView })}&${queryParams({ ...query, page: 1 })}`
    : buildQuery({ analysis_type: analysisType, use_filtered_view: useFilteredView });
  return fetchJson<SeRankingSummaryResponse>(`/seranking/summary?${params}`);
}

export function runSeRankingAnalysis(
  analysisType: "cms_migration" | "domain_migration",
  confirm = false,
  query?: LeadQuery,
  useFilteredView = false,
) {
  return fetchJson<SeRankingRunResponse>("/seranking/analyze", {
    method: "POST",
    body: JSON.stringify({ analysis_type: analysisType, confirm, filters: query ?? {}, use_filtered_view: useFilteredView }),
  });
}

export function refreshSeRankingAnalysis(
  analysisType: "cms_migration" | "domain_migration",
  query?: LeadQuery,
  useFilteredView = false,
) {
  return fetchJson<SeRankingRunResponse>("/seranking/refresh", {
    method: "POST",
    body: JSON.stringify({ analysis_type: analysisType, filters: query ?? {}, use_filtered_view: useFilteredView }),
  });
}

export function previewManualSeRankingAnalysis(payload: SeRankingManualRequest) {
  return fetchJson<SeRankingManualPreviewResponse>("/seranking/manual/preview", {
    method: "POST",
    body: JSON.stringify({
      first_month: payload.firstMonth,
      second_month: payload.secondMonth,
      root_domains: payload.rootDomains ?? [],
      use_selected_tray: payload.useSelectedTray ?? true,
    }),
  });
}

export function runManualSeRankingAnalysis(payload: SeRankingManualRequest) {
  return fetchJson<SeRankingManualRunResponse>("/seranking/manual/run", {
    method: "POST",
    body: JSON.stringify({
      first_month: payload.firstMonth,
      second_month: payload.secondMonth,
      root_domains: payload.rootDomains ?? [],
      use_selected_tray: payload.useSelectedTray ?? true,
    }),
  });
}

export function fetchSiteStatusSummary() {
  return fetchJson<SiteStatusSummaryResponse>("/site-status/summary");
}

export function runSiteStatusCheck(confirm: boolean) {
  return fetchJson<SiteStatusRunResponse>("/site-status/analyze", {
    method: "POST",
    body: JSON.stringify({ confirm }),
  });
}

export function refreshSiteStatusCheck() {
  return fetchJson<SiteStatusRunResponse>("/site-status/refresh", {
    method: "POST",
  });
}

export function fetchScreamingFrogSummary(crawlMode: "bounded_audit" | "deep_audit") {
  return fetchJson<ScreamingFrogSummaryResponse>(`/screamingfrog/summary?${buildQuery({ crawl_mode: crawlMode })}`);
}

export function runScreamingFrogAudit(crawlMode: "bounded_audit" | "deep_audit", confirm: boolean) {
  return fetchJson<ScreamingFrogRunResponse>("/screamingfrog/analyze", {
    method: "POST",
    body: JSON.stringify({ crawl_mode: crawlMode, confirm }),
  });
}

export function refreshScreamingFrogAudit(crawlMode: "bounded_audit" | "deep_audit") {
  return fetchJson<ScreamingFrogRunResponse>("/screamingfrog/refresh", {
    method: "POST",
    body: JSON.stringify({ crawl_mode: crawlMode }),
  });
}

export function fetchScreamingFrogJobStatus(batchId: string) {
  return fetchJson<ScreamingFrogJobBatch>(`/screamingfrog/jobs/${encodeURIComponent(batchId)}`);
}

export function stopScreamingFrogJobBatch(batchId: string) {
  return fetchJson<ScreamingFrogJobBatch>(`/screamingfrog/jobs/${encodeURIComponent(batchId)}/stop`, {
    method: "POST",
  });
}
