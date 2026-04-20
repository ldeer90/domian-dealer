export type SummaryResponse = {
  processed_at: string;
  target_countries: string[];
  overview: {
    unique_leads: number;
    country_counts: Record<string, number>;
    event_platform_counts: Record<string, number>;
    top_corridors: Array<{ corridor: string; count: number }>;
    migration_pair_count: number;
    sales_bucket_counts: Record<string, number>;
  };
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
  selectedOnly: boolean;
  hasSeRankingAnalysis: boolean;
  seRankingAnalysisTypes: string[];
  seRankingOutcomeFlags: string[];
  timelinePlatforms: string[];
  timelineEventTypes: TimelineEventType[];
  timelineDateField: TimelineDateField;
  timelineSeenFrom: string;
  timelineSeenTo: string;
  cmsMigrationFrom: string;
  cmsMigrationTo: string;
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
  se_ranking_market: string;
  se_ranking_migration_date: string;
  se_ranking_baseline_month: string;
  se_ranking_comparison_month: string;
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
};

export type SeRankingSummaryResponse = {
  analysisType: "cms_migration" | "domain_migration";
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
