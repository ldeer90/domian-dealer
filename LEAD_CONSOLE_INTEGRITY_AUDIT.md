# Lead Console Integrity Audit

## Purpose

This audit exists to keep DOMAIN DEALER trustworthy as a lead console.

The main question is:

`If the user applies a condition, does every surface show the same truth?`

The audited surfaces are:

- filter sidebar
- scoped filter options
- worksheet results
- export CSV
- analytics
- timeline cohort
- tray selection and selected-only mode
- presets
- right drawer summary
- SE Ranking summary fields

## Canonical Truth Model

### Canonical filter contract

The canonical backend filter object is built by:

- `/Users/laurencedeer/Desktop/BuiltWith/backend/main.py`
  - `normalize_lead_filters(...)`
  - `build_lead_filters(...)`

This is now the single internal contract for:

- geography and tiers
- live/current state
- migration filters
- timing filters
- advanced grouped-tech filters
- selected-only
- SE Ranking filters

The canonical frontend query shape is:

- `/Users/laurencedeer/Desktop/BuiltWith/frontend/src/types.ts`
  - `LeadQuery`
- `/Users/laurencedeer/Desktop/BuiltWith/frontend/src/App.tsx`
  - `initialQuery`
  - `normalizeLeadQuery(...)`
- `/Users/laurencedeer/Desktop/BuiltWith/frontend/src/api.ts`
  - serialized query coverage check for all `LeadQuery` keys

### Source-of-truth fields

- Live/current state:
  - `leads.current_platforms`
  - live-only rule = `coalesce(trim(current_platforms), '') != ''`
- Selected state:
  - `state.export_tray_items`
  - frontend row selection derives from tray membership
- CMS migration summary:
  - `leads.cms_migration_*`
- Domain migration summary:
  - `domain_migration_best_match_ui`
  - joined into lead rows for worksheet/export/detail
- Latest SE Ranking state:
  - `state.seranking_analysis_snapshots`
  - joined into lead rows for worksheet/export/detail
- Advanced grouped technologies:
  - `leads.marketing_platforms`
  - `leads.crm_platforms`
  - `leads.payment_platforms`
  - `leads.hosting_providers`
  - `leads.agencies`
  - `leads.ai_tools`
  - `leads.compliance_flags`

## Current Audit Status

### Golden audit cases

The reusable audit harness lives at:

- `/Users/laurencedeer/Desktop/BuiltWith/tools/audit_integrity.py`

The golden cases live at:

- `/Users/laurencedeer/Desktop/BuiltWith/tools/integrity_golden_cases.json`

Current green cases:

- `agency_au_central_coast_web_design`
- `live_neto_au_tier_abc`
- `migration_or_au_ab_contact`
- `advanced_ai_scope_au_ab`
- `timeline_neto_au_abc`
- `se_ranking_cms_migration`
- `se_ranking_manual_comparison`
- `selected_only_small_agency_mutation`

These currently verify parity between:

- worksheet and export row counts
- worksheet and analytics counts
- worksheet and timeline unique-domain counts where timeline scoping is active
- worksheet first-page sample and export sample ordering
- worksheet summary fields and drawer summary fields
- SE Ranking mode and comparison-month fields between worksheet and drawer
- scoped advanced filter options and the actual filtered result-set values
- select-all-filtered and selected-only behavior on an isolated temp state DB

### Resolved audit findings

These were found and fixed during the audit pass:

1. Positional filter drift in backend call sites
- Risk: different endpoints could silently apply different filters.
- Fix: all internal filtering now flows through a normalized filter object instead of endpoint-specific positional plumbing.

2. `liveSitesOnly` drift across surfaces
- Risk: worksheet, presets, tray selection, and scoped options could disagree.
- Fix: `liveSitesOnly` now lives in the canonical filter contract and is included in normalization, presets, and frontend request state.

3. Frontend effect dependency drift
- Risk: new query fields could update UI state without reloading worksheet or scoped options.
- Fix: worksheet, analytics, timeline, and scoped filter option loading now derive from memoized normalized query objects instead of hand-maintained dependency lists.

4. Analytics ignored timeline-platform filtering
- Risk: worksheet and timeline could show a narrowed set while analytics still reported a broad one.
- Fix: shared filtered-row fetching now applies the same timeline join path used by the worksheet/export flow.

5. Preset payload drift
- Risk: new filter fields could disappear when saving/loading presets.
- Fix: preset query payloads are normalized on save and on read.

## Known Gaps

These are still open or only partially covered:

1. The audit harness is API-level, not browser-visual.
- It proves data and endpoint parity.
- It does not inspect layout, contrast, sticky headers, or drawer presentation regressions.

2. Timeline parity is only meaningful when a timeline platform filter is active.
- Without `timeline_platforms`, the timeline endpoint has different semantics than the worksheet count.

3. Faceted advanced options currently scope to the active result set.
- This audit treats “options must be a subset of filtered rows” as the correct rule.
- It does not enforce “ignore self-filter when computing the same facet” behavior.

## How To Run

### Run the audit

```bash
cd /Users/laurencedeer/Desktop/BuiltWith
python3 tools/audit_integrity.py
```

### Regenerate the golden baseline

Only do this when you intentionally accept a new truth baseline:

```bash
cd /Users/laurencedeer/Desktop/BuiltWith
python3 tools/audit_integrity.py --update-golden
```

### Optional reports

```bash
cd /Users/laurencedeer/Desktop/BuiltWith
python3 tools/audit_integrity.py \
  --markdown-out /tmp/domain_dealer_integrity_report.md \
  --json-out /tmp/domain_dealer_integrity_report.json
```

## Acceptance Criteria

The lead console should be considered integrity-safe when:

- worksheet total = export row count for each golden case
- worksheet total = analytics `filteredLeads` for each golden case
- timeline `uniqueDomains` = worksheet total whenever timeline scoping is active
- drawer summary fields match the worksheet row for sampled leads
- scoped advanced options never include values absent from the filtered export set
- select-all-filtered produces the same count as the filtered worksheet and selected-only export
- saved/built-in presets round-trip all current filter fields through normalization

## Service Business Readiness Notes

The current audit makes the console safer for service-business expansion because it clarifies which parts are now stable:

- shared filter contract
- grouped tech filters
- migration summary surfaces
- tray/export behavior
- scoped facet behavior

The next audit expansion for service businesses should add parity coverage for any new fields such as:

- Google Business Profile metrics
- review/rating filters
- local pack or map signals
- booking/call-tracking/service-business stack fields
- local-service migration and post-migration SEO outcome summaries
