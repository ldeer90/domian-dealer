# Full Audit and Crawl Pipeline Review

Generated: `2026-04-24T10:12:15.139294+10:00`

## Summary

- Integrity harness status: `PASS`
- SE snapshot rows: `296`
- Site status rows: `1`
- Screaming Frog rows: `14`

## Pipeline Contracts

### SE Ranking
- Discovery: Tray-driven or manual comparison input -> latest snapshot per root_domain selected in join
- Extraction: analysis_type, analysis_mode, regional_source, traffic deltas, keyword deltas, outcome_flags, captured_at/status/error
- Scoring: Used mainly as supporting evidence and worksheet sorting, not as a deep additive score layer
- Presentation: Worksheet columns, drawer summary, export, filters
- Segmentation: Supports momentum/change buckets but can drift if latest-result semantics are unclear

### Site status
- Discovery: Direct HTTP request to selected tray domains with follow-redirect classification
- Extraction: requested_url, final_url, status_code, status_category, redirect_count, checked_at, error_message
- Scoring: Currently more cleanup-oriented than score-driving
- Presentation: Worksheet, filters, drawer, export
- Segmentation: Useful for dead/redirect cleanup and live-site exclusions; weak as a standalone outreach signal

### Screaming Frog
- Discovery: CMS-aware seed discovery -> redirect-aware homepage -> sitemap/category ranking -> bounded seed cap
- Extraction: crawl/result quality, title/meta/canonical/H1 flags, schema/internal error counts, collection intelligence, heading intelligence, seed diagnostics
- Scoring: Primary enrichment score layer via opportunity score, issue family, issue reason, outreach hooks, lead score bonus
- Presentation: Worksheet, drawer, full audit workspace, export, filters
- Segmentation: Strongest audit pipeline for cold-email segmentation, but also the highest-risk for extraction and scoring drift

## Current Findings

- `P1` Collection intro extraction likely still mixes real gaps with extraction misses: `missing_intro` appears on 7/14 saved audits, so extraction quality should be reviewed before treating the signal as universally strong.
- `P1` CMS-specific collection title signals are still underrepresented in the worksheet: The sheet auto-shows many Screaming Frog diagnostics, but it does not currently auto-promote collection title optimisation, which is one of the strongest direct outreach signals.
- `P2` Site status appears useful mainly for cleanup, not segmentation: The current stored site-status categories are narrow enough that they likely work better as exclusions and hygiene checks than as first-class cold-email buckets.
- `P2` Current parity harness is API-strong but enrichment-light: The existing integrity cases cover worksheet/export/analytics/timeline/preset parity well, but they do not yet validate Screaming Frog, site status, or deeper worksheet usefulness decisions with the same rigor.

## Crawl-Quality Matrix

### SE Ranking
- signalFamilies: `["analysis mode/type", "traffic deltas", "keyword deltas", "price deltas", "outcome flags", "status/error"]`
- snapshotRows: `296`
- latestStatusCounts: `{"success": 283, "partial": 11, "error": 2}`
- topOutcomeFlags: `{"traffic_down": 141, "keywords_down": 132, "traffic_up": 114, "traffic_down_20_plus": 110, "traffic_up_20_plus": 87, "keywords_up": 87, "keywords_down_20_plus": 70, "traffic_up_50_plus": 66, "keywords_flat": 64, "keywords_up_20_plus": 46}`

### Site status
- signalFamilies: `["status category", "status code", "redirect count", "final URL", "error"]`
- snapshotRows: `1`
- statusCategoryCounts: `{"redirect": 1}`

### Screaming Frog
- signalFamilies: `["crawl quality", "seed diagnostics", "technical issue flags", "collection intelligence", "heading intelligence", "opportunity scoring"]`
- snapshotRows: `14`
- statusCounts: `{"success": 12, "partial": 1, "error": 1}`
- resultQualityCounts: `{"useful": 12, "partial": 1, "error": 1}`
- resultReasonCounts: `{"": 12, "redirect_only_homepage": 1, "bounded crawl only captured the homepage": 1}`
- primaryIssueFamilies: `{"product_metadata_gap": 6, "collection_content_gap": 5, "technical_breakage": 2, "schema_gap": 1}`
- collectionIntroStatuses: `{"missing_intro": 7, "strong_intro": 4, "": 2, "below_grid_copy": 1}`
- titleOptimizationStatuses: `{"unknown": 8, "customised": 2, "": 2, "term_plus_site": 1, "default_exact": 1}`
- topSchemaIssueFlags: `{"missing_schema": 14}`
- topCollectionIssueFlags: `{"missing_intro": 7}`
- topDefaultTitleFlags: `{"default_exact": 1, "term_plus_site": 1}`
- topHeadingIssueFlags: `{"missing_h1": 8, "repeated_heading_text": 7, "weak_heading_depth": 5, "multiple_h1": 4, "generic_heading_patterns": 3, "duplicate_h1_across_pages": 2}`

## Scoring Review

- `homepage_status_category` (Screaming Frog): weight `High`, issue family `technical_breakage`, outreach relevance `High`
- `internal_4xx_count/internal_5xx_count` (Screaming Frog): weight `High`, issue family `technical_breakage`, outreach relevance `High`
- `schema_page_count` (Screaming Frog): weight `Medium`, issue family `schema_gap`, outreach relevance `Medium`
- `collection_intro_status` (Screaming Frog): weight `High`, issue family `collection_content_gap`, outreach relevance `High`
- `title_optimization_status` (Screaming Frog): weight `Medium`, issue family `default_collection_title`, outreach relevance `High`
- `title_issue_flags/meta_issue_flags` (Screaming Frog): weight `Medium`, issue family `product_metadata_gap`, outreach relevance `Medium`
- `heading_issue_flags` (Screaming Frog): weight `Medium`, issue family `heading_hygiene`, outreach relevance `Medium`
- `traffic_delta_percent/keywords_delta_percent` (SE Ranking): weight `Support`, issue family `Outcome only`, outreach relevance `Medium`
- `site_status_category` (Site status): weight `Support`, issue family `Cleanup only`, outreach relevance `Low`
- `domain_migration_status/cms_migration_status` (Core lead): weight `High`, issue family `Migration trigger`, outreach relevance `High`

## Spreadsheet Review

- Default visible columns: `country, vertical, current_platforms, cms_migration, cms_migration_date, domain_migration, domain_migration_date, sales_buckets, technology_spend, total_score, priority_tier, domain_fingerprint_strength, domain_shared_signals, reason, contact_status`
- Auto Screaming Frog columns: `sf_status, sf_config, sf_quality, sf_score, sf_primary_issue, sf_issue_signals, sf_strengths, sf_heading_health, sf_collection_title_signal, sf_collection_intro, sf_homepage_status, sf_internal_errors, sf_checked`
- Promote: `sf_title_optimization, sf_collection_intro, sf_issue_signals, sf_strengths, sf_heading_health`
- Keep: `sf_status, sf_score, sf_primary_issue, sf_homepage_status, sf_internal_errors, sf_checked`
- Demote: `sf_config, sf_quality, sf_pages_crawled, site_status_code, site_final_url`

## Implementation Roadmap

- Extend the existing integrity harness with enrichment-specific golden cases for Screaming Frog, site status, and SE Ranking display parity.
- Add a crawl-quality audit layer that checks seed selection, weak/partial/error classification, and 429-required-recrawl behavior.
- Refactor scoring review into an explicit matrix and rebalance over-dominant issue families before adding more outreach buckets.
- Promote collection title, collection intro, heading health, and compact issue/strength summaries in the worksheet default SF view.
- Demote or remove low-signal diagnostics from primary spreadsheet space when they do not improve segmentation or shortlist review.
