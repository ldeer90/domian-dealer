# BuiltWith Download And Merge Plan

## Goal

Download every BuiltWith export that helps you create high-volume, high-quality prospecting buckets, then merge them into one reusable lead database built around `root_domain`.

## What To Download

There are two layers:

1. Base report types you should create inside BuiltWith
2. Export files you should download from those reports

## 1. Base Reports To Create In BuiltWith

These are the report families worth building because they can be reused across many bucket variations.

### Core technology reports

Create technology reports for the main ecosystems you want to sell into.

Use these for:

- recent platform adopters
- high-value current stacks
- vertical slices
- location slices
- spend / employee filtering

### Competitor comparison reports

Create comparison reports for likely switching or churn-intent ecosystems.

Use these for:

- churn-risk leads
- switching-intent leads
- migration timing leads

### "Using X but not Y" gap reports

Create gap reports where a company uses one meaningful technology but lacks another.

Best examples:

- uses major framework but no CDN/WAF
- uses paid media tech but no analytics/tag manager
- uses complex stack but weak measurement/security setup

Use these for:

- performance hooks
- tracking / attribution hooks
- infrastructure maturity hooks

### Keyword reports

Create keyword-based homepage reports around commercial intent phrases.

Best examples:

- pricing
- quote
- consultation
- book online
- demo
- free trial

Use these for:

- service-business buckets
- SaaS / lead-gen buckets
- local commercial-intent slices

### Staging / development / testing reports

Use these for:

- pre-launch SEO risk
- redesign / relaunch trigger lists
- technical audit offers

### SaaS pricing reports

Use these for:

- pricing-page businesses
- product-led growth companies
- comparison / alternative SEO hooks

### Inferred language reports

Use these for:

- multilingual sites
- non-English markets
- localisation / hreflang opportunities

### Multinational company reports

Use these for:

- cross-market websites
- international SEO outreach
- enterprise-style account lists

### Many subdomains reports

Use these for:

- complex architecture
- crawl-budget and cannibalisation risks
- large-site technical SEO leads

### Verified profile reports

Use these for:

- higher-trust / larger-brand prioritisation
- stronger-fit outbound targets

### Issues / diagnostics filtered reports

Apply BuiltWith issue filters to find:

- low performance
- low SEO score
- accessibility issues
- best-practices issues

Use these for:

- technical-audit hooks
- low-performance outreach buckets

### Related-domain / shared identifier reports

Use these for:

- brand portfolios
- shared ownership networks
- group-level SEO opportunities

## 2. Export Files You Should Download

These are the actual downloads you should pull from BuiltWith.

## Essential On Almost Every Bucket

### `bw_meta.csv`

Download for every bucket you may contact.

This is your main firmographic and contact export.

Capture:

- domain
- location_on_site
- company
- tech spend
- employees
- vertical
- contact details
- social/profile links
- city / state / postcode / country
- first detected
- last found
- first indexed
- last indexed
- any traffic / rank fields available
- any score fields available

### `bw_custom_stack.csv`

Download for every priority bucket.

This is the fingerprint export that makes later segmentation possible.

Select a stable set of columns such as:

- primary CMS
- ecommerce platform
- hosting provider
- CDN / WAF
- analytics stack
- tag manager present
- framework family
- marketing automation platform
- inferred language
- AI / homepage flag if relevant
- multinational flag
- many subdomains flag
- verified profile flag
- SaaS pricing band

## Essential For Timing / Migrations / Trigger Buckets

### `bw_live_tech_long.csv`

Download for:

- recent migrators
- migration cohorts
- churn-risk lists
- staging / rebuild lists

This gives you per-technology timing detail.

Capture:

- domain
- technology category
- technology name
- first detected
- last found
- first indexed
- last indexed
- location on site if available

## Essential For Outreach-Ready Contacting

### `bw_people_crm.csv`

Download for:

- top A/B outreach slices
- lists you want to push into a CRM

Capture:

- domain
- company
- person name
- person title
- person email
- person phone
- linkedin URL
- city / state / country
- source bucket id
- export date

### `bw_postal.csv`

Download only when useful for:

- enterprise accounts
- local-market outreach
- direct mail / territory work

## Essential For Technical Hooks

### `bw_domain_attributes.csv`

Download for:

- measurement-risk buckets
- indexation / sitemap hooks
- tagging / implementation sanity checks

Capture:

- domain
- GTM tag count
- sitemap count
- any other domain attributes available

## Essential For Network Expansion

### `bw_related_domains.csv`

Download for:

- portfolio leads
- agency/group ownership patterns
- related-site expansion

Capture:

- domain
- related domain
- shared identifier type
- shared identifier value

### `bw_related_attributes.csv`

Download alongside related domains.

Capture:

- domain
- attribute type
- attribute value

## Selective / High-Value Only

### `bw_matrix.csv`

Download only for:

- top-tier leads
- large shortlist analysis
- outreach personalisation by stack

This is very useful, but not necessary for every bucket.

## Download Priority By Importance

If you want the shortest high-value stack, download in this order:

1. `bw_meta.csv`
2. `bw_custom_stack.csv`
3. `bw_live_tech_long.csv`
4. `bw_people_crm.csv`
5. `bw_domain_attributes.csv`
6. `bw_related_domains.csv`
7. `bw_related_attributes.csv`
8. `bw_matrix.csv`
9. `bw_postal.csv`

## How We Will Combine The Data

The merge strategy should be simple and durable:

### Core rule

Use `root_domain` as the master key.

Do not dedupe on full URLs, because BuiltWith may export:

- homepage domains
- subdomains
- paths in `location_on_site`

Instead:

- store `root_domain` as the canonical company/site key
- keep `domain` exactly as exported
- keep `location_on_site` separately for context

## Final Dataset Structure

Build one master table called `master_leads.csv`.

Each row should represent one `root_domain`.

Main sections:

- identity
- firmographics
- contactability
- geography
- performance / issue flags
- timing / trigger fields
- platform fingerprint
- related-domain/network flags
- bucket tags
- scoring / priority

## Merge Order

### Step 1: Start from metadata

Use `bw_meta.csv` as the base table because it contains the broadest business context.

Create:

- `domain`
- `root_domain`
- `location_on_site`
- `company`
- `vertical`
- `tech_spend_usd`
- `employees_est`
- `city`
- `state`
- `zip_postcode`
- `country`
- `emails`
- `telephones`
- `first_detected`
- `last_found`
- `first_indexed`
- `last_indexed`

### Step 2: Add stack fingerprint fields

Join `bw_custom_stack.csv` onto the same `root_domain`.

This adds the reusable segmentation fields:

- primary CMS
- ecommerce platform
- CDN/WAF
- analytics stack
- framework family
- tag manager present
- inferred language
- SaaS pricing band
- many subdomains flag
- verified profile flag

### Step 3: Add timing detail

Join or aggregate `bw_live_tech_long.csv`.

Because this file is one domain-to-many-technologies, do not flatten it naively.

Instead derive summary fields such as:

- `first_detected_any`
- `last_found_any`
- `recent_adopter_flag`
- `recent_drop_flag`
- `migration_signal_flag`

Keep the raw long file too, because it is your evidence source.

### Step 4: Add contact rows separately

Do not force all people into the master company row.

Use `bw_people_crm.csv` as a child table linked by `root_domain`.

That gives you:

- one company-level master table
- one person/contact table

This avoids messy duplicate rows when a domain has multiple contacts.

### Step 5: Add technical hook fields

Join `bw_domain_attributes.csv` by `root_domain`.

This creates useful derived flags such as:

- `low_sitemap_count_flag`
- `high_gtm_complexity_flag`
- `measurement_risk_flag`

### Step 6: Add network intelligence

Do not flatten all related domains into the master row.

Keep:

- `related_domains.csv` as a relationship table
- `related_attributes.csv` as an identifier table

Then derive summary fields back into the master table:

- `related_domain_count`
- `portfolio_flag`
- `shared_identifier_types`

### Step 7: Add bucket membership

A domain will appear in many buckets, so do not store this as a single field only.

Use:

- a master field like `bucket_ids` with pipe-delimited values
- a separate mapping table `domain_bucket_map.csv`

That gives you both simplicity and flexibility.

## Files To Maintain

You should end up with these core processed files:

### `master_leads.csv`

One row per root domain.

### `domain_bucket_map.csv`

One row per `root_domain x bucket_id`.

This lets you see every reason a lead belongs in your database.

### `contacts.csv`

Derived from `bw_people_crm.csv`.

One row per person/contact.

### `tech_timing_long.csv`

Standardised version of `bw_live_tech_long.csv`.

One row per `root_domain x technology`.

### `related_domain_map.csv`

One row per `root_domain x related_domain`.

## Deduplication Rules

Apply these consistently:

1. Strip protocol
2. Strip paths
3. Lowercase domains
4. Derive `root_domain`
5. Keep subdomain/path detail separately if useful
6. Dedupe company-level table on `root_domain`
7. Never dedupe people or related domains into the same row structure as company data

## What Should Be Aggregated Vs Kept Raw

Aggregate into `master_leads.csv`:

- firmographics
- geography
- summary timing flags
- summary stack fields
- summary contact counts
- summary related-domain counts
- bucket tags

Keep raw or child-table format:

- people records
- per-technology timing records
- per-related-domain relationship records
- postal addresses
- full matrix exports

## Recommended Combination Logic

When multiple exports disagree:

1. Prefer the newest export date
2. Prefer non-empty values over empty values
3. Preserve raw source files
4. Record `source_report` and `export_date`
5. Keep important multi-value fields delimited rather than overwriting them blindly

## The Minimum Viable Pipeline

If we keep this lean, the practical workflow is:

1. Download raw BuiltWith exports by bucket
2. Standardise filenames and export dates
3. Canonicalise domains into `root_domain`
4. Merge `bw_meta` into a master base
5. Join `bw_custom_stack`
6. Derive timing flags from `bw_live_tech_long`
7. Load `bw_people_crm` into contacts table
8. Load `bw_domain_attributes` into technical-flags layer
9. Load `bw_related_domains` and `bw_related_attributes` into relationship tables
10. Build `domain_bucket_map`
11. Create final `master_leads.csv`

## Best Practical Advice

If your aim is maximum value from one month, do not try to export every possible report equally.

Pull all of these broadly:

- `bw_meta.csv`
- `bw_custom_stack.csv`

Pull these heavily for trigger buckets:

- `bw_live_tech_long.csv`
- `bw_people_crm.csv`

Pull these selectively where the hook matters:

- `bw_domain_attributes.csv`
- `bw_related_domains.csv`
- `bw_related_attributes.csv`
- `bw_matrix.csv`
- `bw_postal.csv`

That will give you the best balance of quality, scale, and manageable cleanup.
