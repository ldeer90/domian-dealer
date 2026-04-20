# BuiltWith One-Month Sprint Plan

## Objective

Build the highest-quality and highest-volume lead database possible during one month of BuiltWith Pro access, while leaving behind a reusable prospecting system that still works after the subscription ends.

## Success Metrics

- 5 to 8 strong base report families created in week 1
- 50+ reusable bucket variants created from those base reports
- Daily Net New flow running from day 8 onward
- A scored `master_leads.csv` with deduped domains and bucket tags
- A-tier and B-tier lists enriched enough for outreach
- A repeatable cookbook so the process can be reused later

## Working Principle

Use BuiltWith for breadth and trigger discovery.
Use SE Ranking only on the best slices for value and prioritisation.
Export aggressively and structure data from day 1 so nothing is trapped inside the subscription.

## Recommended Folder Structure

Create and keep everything under:

`/Users/laurencedeer/Desktop/BuiltWith`

Suggested structure:

- `raw/builtwith/meta/`
- `raw/builtwith/custom/`
- `raw/builtwith/live-tech/`
- `raw/builtwith/crm/`
- `raw/builtwith/domain-attributes/`
- `raw/builtwith/related/`
- `raw/seranking/overview/`
- `raw/seranking/keywords/`
- `raw/seranking/gaps/`
- `processed/`
- `docs/`
- `outreach/`

## The Plan

### Phase 1: Foundation (Days 1 to 3)

Set up the system before chasing volume.

1. Define your core market slices.
2. Create naming rules for every report and export.
3. Build only a small number of base reports first.
4. Standardise export fields immediately.
5. Start a master sheet with one row per root domain.

Base report families to create first:

- Core CMS/platform reports
- Competitor comparison / switching reports
- Keyword-based service reports
- Staging/dev/testing reports
- SaaS pricing reports
- International / inferred language / multinational reports
- Gap reports like "uses X but not Y"

### Phase 2: Expansion (Days 4 to 10)

Turn each base report into many filtered variants instead of constantly creating new reports.

Primary filters to fan out with:

- Country / region / city
- Vertical
- Spend
- Employees
- Traffic / rank
- Detection date
- Issues / diagnostics
- TLD

Priority bucket types:

- Recent migrators
- Migration cohorts
- Churn-risk / switch-intent
- Staging/dev/test
- High-value current stacks
- Gap-based measurement leads
- Gap-based CDN/WAF/security leads
- Low performance / low SEO score
- Internationalisation opportunities
- SaaS pricing-page leads
- Many subdomains / complex architecture
- Network / related-domain portfolios

Target output for this phase:

- 20 to 30 usable buckets
- First working scoring model
- First outreach-ready A/B list

### Phase 3: Net New Production (Days 8 to 21)

Once reports pass the 24-hour mark, stop relying on full rebuilds and shift into a daily Net New rhythm.

Daily operating cadence:

1. Pull Net New from eligible reports.
2. Export `bw_meta` and `bw_custom_stack` for any contactable slice.
3. Export live-tech only for timing-sensitive buckets.
4. Dedupe into `master_leads.csv`.
5. Enrich only A/B tiers in SE Ranking.
6. Push the best leads into CRM/outreach format.

Rule of thumb:

- BuiltWith does discovery and list generation.
- SE Ranking does proof, value, and prioritisation.

### Phase 4: Final Harvest and Archive (Days 22 to 30)

Stop inventing too many new buckets late in the month.
Use the last week to capture everything worth keeping.

Final-week priorities:

- Top 200 A-tier leads fully enriched
- Matrix export for the strongest accounts
- Related-domain mapping for network opportunities
- Final archive of all raw exports
- Final scoring pass
- Bucket cookbook with exact recipes and hooks

## Best Order of Attack

If the goal is both quality and volume, work in this order:

1. High-intent trigger buckets
2. High-value stack buckets
3. Geography and vertical slicing
4. Gap-based buckets
5. International and complex architecture buckets
6. Portfolio / related-domain expansion

This order works because trigger-led lists convert better early, while the broader variants give you scale after the core system is stable.

## Report Creation Strategy

Do not create dozens of independent reports upfront.
Create a few high-quality base reports and multiply them with filters.

Recommended week-1 base reports:

1. Primary CMS/platform family reports in the markets you serve
2. Competitor comparison reports for likely switching ecosystems
3. Keyword reports for broad commercial phrases
4. Staging/dev/testing report
5. SaaS pricing report
6. Inferred language + multinational report
7. Gap reports for analytics, CDN/WAF, security, tag management

## Scoring Model

Use a simple 100-point system:

- Commercial value: 25
- Visibility/value: 20
- Opportunity: 20
- Urgency trigger: 25
- Reachability: 10

Suggested tiering:

- A: 80 to 100
- B: 60 to 79
- C: 40 to 59
- D: below 40

## Highest-Value Export Set

Run these consistently:

- `bw_meta.csv`
- `bw_custom_stack.csv`
- `bw_live_tech_long.csv`
- `bw_people_crm.csv`
- `bw_domain_attributes.csv`
- `bw_related_domains.csv`
- `bw_related_attributes.csv`

Run these selectively:

- `bw_matrix.csv`
- `bw_postal.csv`

## Canonical Data Model

Every domain should eventually resolve into:

- Identity
- Firmographics
- Contacts
- Timing fields
- Performance fields
- Platform fingerprint
- Geography
- Bucket tags
- Score and tier
- SE Ranking enrichment

Core rule:

Dedupe on `root_domain`, not full URL, but preserve `location_on_site` separately.

## Daily Rhythm

Use this as the default working loop:

1. Build or refresh 2 to 4 priority buckets
2. Export raw BuiltWith files
3. Merge and dedupe
4. Score and tier
5. Enrich only A/B tiers with SE Ranking
6. Export outreach-ready people lists
7. Log what recipe created each bucket

## What "Highest Quality + Highest Volume" Actually Means

Highest volume does not mean exporting the biggest possible unsorted lists.
It means:

- broad discovery through BuiltWith filters
- disciplined dedupe
- stable bucket naming
- good timing signals
- lightweight scoring
- selective enrichment

That combination is what lets you keep both scale and usability.

## Recommended Immediate Next Steps

1. Create the folder structure and naming convention.
2. Define your first 5 to 8 base report families.
3. Decide your initial serviceable geographies.
4. Build the master CSV schema before large exports start.
5. Start with trigger-heavy buckets first: migration, staging, churn, low-performance.

## Suggested Day 1 Deliverables

- Folder structure live
- Bucket map document
- Export schema locked
- First 5 to 8 base reports chosen
- First 10 to 15 bucket variants planned
- Initial scoring rubric drafted

## Recommendation

If we want to execute this properly, the best next move is for me to build the working assets in this folder next:

- a bucket map
- a naming convention
- a `master_leads.csv` schema
- a daily operating checklist
- an outreach hook sheet

That would turn this from a strategy note into a working system.
