export type SummaryResponse = {
  processed_at: string;
  target_countries: string[];
  source_coverage?: Array<{
    platform: string;
    hasCurrent: boolean;
    hasRecent: boolean;
    hasRemoved: boolean;
    currentFiles: number;
    recentFiles: number;
    removedFiles: number;
    quarantinedFiles: number;
    rowCount: number;
    targetRows: number;
    confidence: string;
    timingQuality: string;
    notes: string[];
  }>;
  overview: {
    unique_leads: number;
    country_counts: Record<string, number>;
    event_platform_counts: Record<string, number>;
    top_corridors: Array<{ corridor: string; count: number }>;
    migration_pair_count: number;
    sales_bucket_counts: Record<string, number>;
  };
};

export type HealthResponse = {
  status: "ok" | "degraded";
  started_at: string;
  state_db_ready: boolean;
  data_db_ready: boolean;
  lead_query_ready: boolean;
  worker_running: boolean;
  active_batch_id: string;
  active_job_id: string;
  active_process_running: boolean;
  error: string;
};

export type FilterOptions = {
  countries: string[];
  tiers: string[];
  verticals: string[];
  currentPlatforms: string[];
  recentPlatforms: string[];
  removedPlatforms: string[];
  timelinePlatforms: string[];
  salesBuckets: string[];
  domainMigrationStatuses: string[];
  domainConfidenceBands: string[];
  domainFingerprintStrengths: string[];
  domainTldRelationships: string[];
  cmsMigrationStatuses: string[];
  cmsConfidenceLevels: string[];
  seRankingAnalysisTypes: string[];
  seRankingOutcomeFlags: string[];
  siteStatusCategories: string[];
  screamingFrogStatuses: string[];
  screamingFrogHomepageStatuses: string[];
  screamingFrogTitleFlags: string[];
  screamingFrogMetaFlags: string[];
  screamingFrogCanonicalFlags: string[];
  marketingPlatforms: string[];
  crmPlatforms: string[];
  paymentPlatforms: string[];
  hostingProviders: string[];
  agencies: string[];
  aiTools: string[];
  complianceFlags: string[];
};

export type SortDirection = "asc" | "desc";
export type TimelineGranularity = "week" | "month" | "quarter";
export type TimelineEventType = "current_detected" | "recently_added" | "no_longer_detected";
export type TimelineDateField = "first_seen" | "last_seen";
export type MigrationTimingOperator = "and" | "or";

export type LeadQuery = {
  search: string;
  exactDomain: string;
  countries: string[];
  tiers: string[];
  currentPlatforms: string[];
  recentPlatforms: string[];
  removedPlatforms: string[];
  verticals: string[];
  salesBuckets: string[];
  liveSitesOnly: boolean;
  migrationOnly: boolean;
  hasDomainMigration: boolean;
  hasCmsMigration: boolean;
  domainMigrationStatuses: string[];
  domainConfidenceBands: string[];
  domainFingerprintStrengths: string[];
  domainTldRelationships: string[];
  cmsMigrationStatuses: string[];
  cmsConfidenceLevels: string[];
  hasContact: boolean;
  hasMarketing: boolean;
  hasCrm: boolean;
  hasPayments: boolean;
  marketingPlatforms: string[];
  crmPlatforms: string[];
  paymentPlatforms: string[];
  hostingProviders: string[];
  agencies: string[];
  aiTools: string[];
  complianceFlags: string[];
  minSocial: string;
  minRevenue: string;
  minEmployees: string;
  minSku: string;
  minTechnologySpend: string;
  selectedOnly: boolean;
  hasSeRankingAnalysis: boolean;
  seRankingAnalysisTypes: string[];
  seRankingOutcomeFlags: string[];
  hasSiteStatusCheck: boolean;
  siteStatusCategories: string[];
  hasScreamingFrogAudit: boolean;
  screamingFrogStatuses: string[];
  screamingFrogHomepageStatuses: string[];
  screamingFrogTitleFlags: string[];
  screamingFrogMetaFlags: string[];
  screamingFrogCanonicalFlags: string[];
  hasScreamingFrogInternalErrors: boolean;
  hasScreamingFrogLocationPages: boolean;
  hasScreamingFrogServicePages: boolean;
  timelinePlatforms: string[];
  timelineEventTypes: TimelineEventType[];
  timelineDateField: TimelineDateField;
  timelineSeenFrom: string;
  timelineSeenTo: string;
  cmsMigrationFrom: string;
  cmsMigrationTo: string;
  cmsUnchangedYears: string;
  domainMigrationFrom: string;
  domainMigrationTo: string;
  migrationTimingOperator: MigrationTimingOperator;
  timelineGranularity: TimelineGranularity;
  page: number;
  pageSize: number;
  sortBy: string;
  sortDirection: SortDirection;
};

export type ContactStatus = {
  hasEmail: boolean;
  hasPhone: boolean;
  hasPeople: boolean;
};

export type Lead = {
  root_domain: string;
  company: string;
  country: string;
  state: string;
  city: string;
  vertical: string;
  technology_spend: string;
  sales_revenue: string;
  employees: string;
  social: string;
  sku: string;
  contact_score: number | string;
  stack_score: number | string;
  trigger_score: number | string;
  total_score: number | string;
  priority_tier: string;
  migration_candidate_flag: number | string;
  migration_window: string;
  geo_confidence: string;
  recently_added_platforms: string[];
  removed_platforms: string[];
  current_platforms: string[];
  current_candidate_platforms: string[];
  likely_current_platforms: string[];
  integrity_flags: string[];
  matched_first_detected: string;
  matched_last_found: string;
  matched_timeline_platforms: string[];
  best_old_domain: string;
  domain_migration_estimated_date: string;
  domain_redirect_first_seen: string;
  domain_redirect_last_seen: string;
  domain_migration_date_source: string;
  domain_migration_status: string;
  domain_migration_reason: string;
  domain_migration_confidence_score: number | string;
  domain_migration_confidence_band: string;
  domain_fingerprint_strength: string;
  domain_migration_candidate_count: number | string;
  domain_shared_signals: string[];
  domain_shared_technologies: string[];
  domain_migration_notes: string;
  domain_fingerprint_notes: string;
  domain_tld_relationship: string;
  domain_migration_warning_flags: string[];
  domain_migration_evidence_flags: string[];
  cms_migration_status: string;
  cms_migration_reason: string;
  cms_old_platform: string;
  cms_new_platform: string;
  cms_migration_confidence: string;
  cms_migration_gap_days: number | string;
  cms_migration_likely_date: string;
  cms_migration_summary: string;
  cms_first_new_detected: string;
  cms_last_old_found: string;
  cms_migration_warning_flags: string[];
  cms_migration_evidence_flags: string[];
  cms_migration_candidate_count: number | string;
  marketing_platforms: string[];
  payment_platforms: string[];
  crm_platforms: string[];
  hosting_providers: string[];
  agencies: string[];
  ai_tools: string[];
  compliance_flags: string[];
  sales_buckets: string[];
  bucket_reasons: string;
  bucket_reasons_list: string[];
  bucket_count: number;
  contact_status: ContactStatus;
  emails: string;
  telephones: string;
  people: string;
  is_selected: boolean;
  se_ranking_analysis_type: string;
  se_ranking_analysis_mode: string;
  se_ranking_date_mode: string;
  se_ranking_market: string;
  se_ranking_migration_date: string;
  se_ranking_baseline_month: string;
  se_ranking_comparison_month: string;
  se_ranking_first_month: string;
  se_ranking_second_month: string;
  se_ranking_date_label_first: string;
  se_ranking_date_label_second: string;
  se_ranking_traffic_before: number | string;
  se_ranking_traffic_last_month: number | string;
  se_ranking_traffic_delta_absolute: number | string;
  se_ranking_traffic_delta_percent: number | string;
  se_ranking_keywords_before: number | string;
  se_ranking_keywords_last_month: number | string;
  se_ranking_keywords_delta_absolute: number | string;
  se_ranking_keywords_delta_percent: number | string;
  se_ranking_price_before: number | string;
  se_ranking_price_last_month: number | string;
  se_ranking_price_delta_absolute: number | string;
  se_ranking_price_delta_percent: number | string;
  se_ranking_outcome_flags: string[];
  se_ranking_checked_at: string;
  se_ranking_status: string;
  se_ranking_error_message: string;
  site_status_category: string;
  site_status_code: number | string;
  site_status_final_url: string;
  site_status_checked_at: string;
  site_status_error: string;
  site_status_redirect_count: number | string;
  screamingfrog_crawl_mode: string;
  screamingfrog_resolved_platform_family: string;
  screamingfrog_resolved_config_path: string;
  screamingfrog_result_quality: string;
  screamingfrog_result_reason: string;
  screamingfrog_seed_strategy: string;
  screamingfrog_seed_count: number | string;
  screamingfrog_sitemap_found: number | string;
  screamingfrog_sitemap_url: string;
  screamingfrog_sitemap_source: string;
  screamingfrog_requested_homepage_url: string;
  screamingfrog_discovered_final_homepage_url: string;
  screamingfrog_checked_at: string;
  screamingfrog_status: string;
  screamingfrog_error_message: string;
  screamingfrog_pages_crawled: number | string;
  screamingfrog_homepage_status: string;
  screamingfrog_homepage_status_code: number | string;
  screamingfrog_title_issue_flags: string[];
  screamingfrog_meta_issue_flags: string[];
  screamingfrog_canonical_issue_flags: string[];
  screamingfrog_internal_3xx_count: number | string;
  screamingfrog_internal_4xx_count: number | string;
  screamingfrog_internal_5xx_count: number | string;
  screamingfrog_has_internal_errors: number | string;
  screamingfrog_location_page_count: number | string;
  screamingfrog_service_page_count: number | string;
  screamingfrog_category_page_count: number | string;
  screamingfrog_product_page_count: number | string;
  screamingfrog_schema_issue_flags: string[];
  screamingfrog_collection_content_issue_flags: string[];
  screamingfrog_product_metadata_issue_flags: string[];
  screamingfrog_default_title_issue_flags: string[];
  screamingfrog_homepage_issue_flags: string[];
  screamingfrog_heading_issue_flags: string[];
  screamingfrog_heading_outline_score: number | string;
  screamingfrog_heading_outline_summary: string;
  screamingfrog_heading_pages_analyzed: number | string;
  screamingfrog_heading_h1_missing_count: number | string;
  screamingfrog_heading_multiple_h1_count: number | string;
  screamingfrog_heading_duplicate_h1_count: number | string;
  screamingfrog_heading_pages_with_h2_count: number | string;
  screamingfrog_heading_generic_heading_count: number | string;
  screamingfrog_heading_repeated_heading_count: number | string;
  screamingfrog_opportunity_score: number | string;
  screamingfrog_primary_issue_family: string;
  screamingfrog_primary_issue_reason: string;
  screamingfrog_outreach_hooks: string[];
  screamingfrog_collection_detection_status: string;
  screamingfrog_collection_detection_confidence: number | string;
  screamingfrog_collection_main_content: string;
  screamingfrog_collection_main_content_method: string;
  screamingfrog_collection_main_content_confidence: number | string;
  screamingfrog_collection_above_raw_text: string;
  screamingfrog_collection_below_raw_text: string;
  screamingfrog_collection_above_clean_text: string;
  screamingfrog_collection_below_clean_text: string;
  screamingfrog_collection_best_intro_text: string;
  screamingfrog_collection_best_intro_position: string;
  screamingfrog_collection_best_intro_confidence: number | string;
  screamingfrog_collection_best_intro_source_type: string;
  screamingfrog_collection_intro_text: string;
  screamingfrog_collection_intro_position: string;
  screamingfrog_collection_intro_status: string;
  screamingfrog_collection_intro_method: string;
  screamingfrog_collection_intro_confidence: number | string;
  screamingfrog_collection_schema_types: string[];
  screamingfrog_collection_schema_types_method: string;
  screamingfrog_collection_schema_types_confidence: number | string;
  screamingfrog_collection_product_count: number | string;
  screamingfrog_collection_product_count_method: string;
  screamingfrog_collection_product_count_confidence: number | string;
  screamingfrog_collection_title_value: string;
  screamingfrog_collection_title_method: string;
  screamingfrog_collection_title_confidence: number | string;
  screamingfrog_collection_h1_value: string;
  screamingfrog_collection_h1_method: string;
  screamingfrog_collection_h1_confidence: number | string;
  screamingfrog_title_optimization_status: string;
  screamingfrog_title_optimization_confidence: number | string;
  screamingfrog_collection_title_rule_family: string;
  screamingfrog_collection_title_rule_match: string;
  screamingfrog_collection_title_rule_confidence: number | string;
  screamingfrog_collection_title_site_name_match: number | string;
  screamingfrog_collection_issue_family: string;
  screamingfrog_collection_issue_reason: string;
  screamingfrog_export_directory: string;
};

export type LeadsResponse = {
  items: Lead[];
  total: number;
  page: number;
  pageSize: number;
  pages: number;
  sortBy: string;
  sortDirection: SortDirection;
};

export type TimelineRow = {
  root_domain: string;
  platform: string;
  first_detected: string;
  last_found: string;
  first_indexed: string;
  last_indexed: string;
  has_current_detected: number | string;
  has_recently_added: number | string;
  has_removed: number | string;
  event_types: string[];
};

export type LeadDetailResponse = {
  lead: Lead & {
    emails: string[];
    telephones: string[];
    people: string[];
    verified_profiles: string[];
  };
  selected: boolean;
  events: Array<Record<string, string>>;
  migrations: Array<Record<string, string>>;
  migrationIntelligence: {
    summary: {
      hasDomainMigration: boolean;
      hasCmsMigration: boolean;
      domainCandidateCount: number;
      cmsCandidateCount: number;
    };
    domainMigration: {
      bestMatch: (Record<string, string> & {
        shared_signal_flags?: string[];
        shared_high_signal_technologies?: string[];
        domain_tld_relationship?: string;
      }) | null;
      candidateShortlist: Array<Record<string, string> & {
        shared_signal_flags?: string[];
        shared_high_signal_technologies?: string[];
        domain_tld_relationship?: string;
      }>;
    };
    cmsMigration: {
      bestPair: Record<string, string> | null;
      candidatePairs: Array<Record<string, string>>;
    };
  };
  domainMigrationV2: {
    bestMatch: (Record<string, string> & {
      shared_signal_flags?: string[];
      shared_high_signal_technologies?: string[];
      domain_tld_relationship?: string;
      domain_migration_warning_flags?: string[];
      domain_migration_evidence_flags?: string[];
    }) | null;
    candidateShortlist: Array<Record<string, string> & {
      shared_signal_flags?: string[];
      shared_high_signal_technologies?: string[];
      domain_tld_relationship?: string;
    }>;
  };
  cmsMigrationV2: {
    bestPair: (Record<string, string> & {
      warning_flags?: string[];
      evidence_flags?: string[];
    }) | null;
    candidatePairs: Array<Record<string, string> & {
      warning_flags?: string[];
      evidence_flags?: string[];
    }>;
  };
  data_quality: {
    leadFlags: string[];
    cmsWarnings: string[];
    domainWarnings: string[];
    notes: string[];
  };
  timelineRows: TimelineRow[];
  exportReady: {
    root_domain: string;
    company: string;
    country: string;
    emails: string[];
    telephones: string[];
    people: string[];
    bucket_reasons: string[];
  };
  seRankingAnalysis: (Record<string, string | number | null> & {
    outcome_flags?: string[];
  }) | null;
  siteStatusCheck: (Record<string, string | number | null>) | null;
  screamingFrogAudit: (Record<string, string | number | null>) | null;
};

export type SiteStatusSummaryResponse = {
  summary: {
    selectedCount: number;
    eligibleCount: number;
    alreadyCheckedCount: number;
    toRunCount: number;
    estimatedRequests: number;
    excluded: Array<{ root_domain: string; reason: string }>;
  };
};

export type SiteStatusRunResponse = SiteStatusSummaryResponse & {
  results: Array<{
    root_domain: string;
    status: string;
    error_message: string;
  }>;
};

export type ScreamingFrogSummaryResponse = {
  crawlMode: "bounded_audit" | "deep_audit";
  summary: {
    selectedCount: number;
    eligibleCount: number;
    alreadyAuditedCount: number;
    toRunCount: number;
    estimatedRuns: number;
    resolvedConfigBreakdown: Array<{ platformFamily: string; label: string; count: number }>;
    excluded: Array<{ root_domain: string; reason: string }>;
  };
  jobBatch?: ScreamingFrogJobBatch | null;
};

export type ScreamingFrogRunResponse = ScreamingFrogSummaryResponse & {
  results: Array<{
    root_domain: string;
    status: string;
    error_message: string;
    resolved_platform_family: string;
    pages_crawled?: number;
    homepage_status_category?: string;
  }>;
};

export type ScreamingFrogJobBatch = {
  batchId: string;
  isActive: boolean;
  counts: Record<string, number>;
  items: Array<{
    id: string;
    batch_id: string;
    root_domain: string;
    crawl_mode: string;
    resolved_platform_family: string;
    status: string;
    message: string;
    requested_homepage_url: string;
    final_homepage_url: string;
    redirect_detected: number | string;
    sitemap_found: number | string;
    sitemap_url: string;
    sitemap_source: string;
    seed_strategy: string;
    seed_count: number | string;
    result_quality: string;
    result_reason: string;
    started_at: string;
    completed_at: string;
    created_at: string;
    updated_at: string;
  }>;
};

export type SeRankingSummaryResponse = {
  analysisType: "cms_migration" | "domain_migration" | "manual_comparison";
  analysisMode?: "migration" | "manual";
  summary: {
    selectedCount: number;
    eligibleCount: number;
    alreadyAnalyzedCount: number;
    toRunCount: number;
    estimatedRequests: number;
    estimatedCredits: number;
    excluded: Array<{ root_domain: string; reason: string }>;
  };
};

export type SeRankingRunResponse = SeRankingSummaryResponse & {
  results: Array<{
    root_domain: string;
    status: string;
    error_message: string;
  }>;
};

export type SeRankingManualRequest = {
  firstMonth: string;
  secondMonth: string;
  rootDomains?: string[];
  useSelectedTray?: boolean;
};

export type SeRankingManualPreviewResponse = {
  analysisType: "manual_comparison";
  analysisMode: "manual";
  firstMonth: string;
  secondMonth: string;
  summary: SeRankingSummaryResponse["summary"];
};

export type SeRankingManualRunResponse = SeRankingManualPreviewResponse & {
  results: SeRankingRunResponse["results"];
};

export type MixDatum = {
  label: string;
  count: number;
};

export type AnalyticsResponse = {
  kpis: {
    filteredLeads: number;
    priorityAB: number;
    recentMigrations: number;
    confirmedDomainMigrations: number;
    possibleDomainMigrations: number;
    confirmedCmsMigrations: number;
    possibleCmsMigrations: number;
  };
  countryMix: MixDatum[];
  tierMix: MixDatum[];
  currentPlatformMix: MixDatum[];
  salesBucketMix: MixDatum[];
  topCorridors: MixDatum[];
};

export type TimelineCohortResponse = {
  summary: {
    totalStarts: number;
    uniqueDomains: number;
    periodCount: number;
    firstPeriod: string | null;
    lastPeriod: string | null;
  };
  series: Array<{
    period: string;
    count: number;
  }>;
  seriesByPlatform: Array<{
    platform: string;
    points: Array<{
      period: string;
      count: number;
    }>;
  }>;
  technologyBreakdown: Array<{
    platform: string;
    count: number;
  }>;
  availableRange: {
    dateField: TimelineDateField;
    minDate: string | null;
    maxDate: string | null;
  };
};

export type Preset = {
  id: string;
  name: string;
  isBuiltin: boolean;
  filters: LeadQuery;
  visibleColumns: string[];
  group: string;
  description: string;
  order: number;
  sort: {
    sortBy: string;
    sortDirection: SortDirection;
  };
  createdAt: string;
  updatedAt: string;
};

export type PresetsResponse = {
  items: Preset[];
};

export type ExportTrayResponse = {
  count: number;
  rootDomains: string[];
  matchedCount?: number;
  addedCount?: number;
  items: Array<{
    root_domain: string;
    company: string;
    country: string;
    priority_tier: string;
    added_at: string;
  }>;
  countryMix: MixDatum[];
  bucketMix: MixDatum[];
};
