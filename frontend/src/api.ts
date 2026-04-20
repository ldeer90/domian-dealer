import type {
  AnalyticsResponse,
  ExportTrayResponse,
  FilterOptions,
  LeadDetailResponse,
  LeadQuery,
  LeadsResponse,
  Preset,
  PresetsResponse,
  SeRankingRunResponse,
  SeRankingSummaryResponse,
  SummaryResponse,
  TimelineCohortResponse,
} from "./types";

const API_BASE = (import.meta.env.VITE_API_BASE as string | undefined) ?? "http://127.0.0.1:8765/api";

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

async function fetchJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    headers: {
      "Content-Type": "application/json",
    },
    ...init,
  });
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return response.json() as Promise<T>;
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
    selected_only: query.selectedOnly,
    has_seranking_analysis: query.hasSeRankingAnalysis,
    seranking_analysis_types: query.seRankingAnalysisTypes,
    seranking_outcome_flags: query.seRankingOutcomeFlags,
    page: query.page,
    page_size: query.pageSize,
    sort_by: query.sortBy,
    sort_direction: query.sortDirection,
  });
}

export function fetchSummary() {
  return fetchJson<SummaryResponse>("/summary");
}

export function fetchFilterOptions() {
  return fetchJson<FilterOptions>("/filter-options");
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
    selected_only: query.selectedOnly,
    has_seranking_analysis: query.hasSeRankingAnalysis,
    seranking_analysis_types: query.seRankingAnalysisTypes,
    seranking_outcome_flags: query.seRankingOutcomeFlags,
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

export function fetchSeRankingSummary(analysisType: "cms_migration" | "domain_migration") {
  return fetchJson<SeRankingSummaryResponse>(`/seranking/summary?${buildQuery({ analysis_type: analysisType })}`);
}

export function runSeRankingAnalysis(analysisType: "cms_migration" | "domain_migration", confirm = false) {
  return fetchJson<SeRankingRunResponse>("/seranking/analyze", {
    method: "POST",
    body: JSON.stringify({ analysis_type: analysisType, confirm }),
  });
}

export function refreshSeRankingAnalysis(analysisType: "cms_migration" | "domain_migration") {
  return fetchJson<SeRankingRunResponse>("/seranking/refresh", {
    method: "POST",
    body: JSON.stringify({ analysis_type: analysisType }),
  });
}
