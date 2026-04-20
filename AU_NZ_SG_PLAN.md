# AU / NZ / Singapore BuiltWith Plan

## Scope

This plan is now limited to websites operating in:

- Australia (`AU`)
- New Zealand (`NZ`)
- Singapore (`SG`)

Everything outside those three countries should be treated as out of scope unless we explicitly decide otherwise later.

## Objective

Build the highest-quality and highest-volume prospecting database possible for:

- Australia
- New Zealand
- Singapore

using BuiltWith exports only, with a strong focus on:

- current install-base coverage
- migration and switching triggers
- commercially serious stacks
- outreach-ready prioritisation

## What We Have Right Now

Current export families already in the folder:

- `Recently Added CMS`
- `No Longer Detected`

These are useful for:

- recent platform adoption
- platform removal
- probable migration detection
- churn / switching-intent signals

What is still missing:

- clean WooCommerce `No Longer Detected` export
- `Current Detected` exports for Shopify, WooCommerce, BigCommerce, Magento, PrestaShop, OpenCart, and other priority platforms

## Core Strategy

Use three data layers:

1. `Current Detected`
   This is the market map.
   It tells us who currently runs each platform in AU/NZ/SG.

2. `Recently Added`
   This is the urgency layer.
   It tells us who has likely just adopted a new platform.

3. `No Longer Detected`
   This is the switching layer.
   It tells us who has likely removed or replaced a platform.

Together, these give us:

- broad TAM-style lists
- trigger-based lead lists
- probable migration corridors

## Geography Rule

Do not trust filenames or folder labels as the geography filter.

Always filter using the `Country` column inside each CSV:

- `AU`
- `NZ`
- `SG`

If `Country` is blank:

- keep only if another reliable signal proves AU/NZ/SG relevance
- otherwise exclude from the priority dataset

## Final Data Model

We will manage the data in four layers.

### 1. Raw exports

Keep every original CSV unchanged.

### 2. Standardised exports

Each file gets:

- standardised filename
- standardised platform label
- standardised event type
- derived `root_domain`
- derived `country_scope_flag`

### 3. Master company table

One row per `root_domain`.

This will hold:

- company identity
- geography
- stack summary
- contactability
- trigger flags
- quality score
- priority tier

### 4. Child / relationship tables

Separate tables for:

- platform events
- migration matches
- contacts
- bucket membership

## Buckets We Will Build

### Bucket Group 1: Current platform lists

These are the broad “who uses what now” lists.

Priority platforms:

- Shopify
- Shopify Plus
- WooCommerce Checkout
- BigCommerce
- Magento
- Magento Enterprise if available
- PrestaShop
- OpenCart

Use cases:

- market map
- install-base prospecting
- vertical slicing
- app-stack slicing

### Bucket Group 2: Recent adoption lists

These come from `Recently Added`.

Use cases:

- post-migration outreach
- recent relaunch support
- recent replatforming checks

### Bucket Group 3: No-longer-detected lists

These come from `No Longer Detected`.

Use cases:

- switch-intent leads
- migration leads
- “something changed here” triggers

### Bucket Group 4: Probable migration corridors

These are created by matching:

- same `root_domain`
- one old platform disappearing
- one new platform appearing

Examples:

- Magento -> Shopify
- BigCommerce -> Shopify
- WooCommerce -> Shopify
- OpenCart -> WooCommerce
- PrestaShop -> Shopify

These should become some of the highest-priority lead lists.

### Bucket Group 5: Commercial stack leads

These come from app and stack fields already in the exports:

- payment platforms
- CRM platform
- marketing automation platform
- hosting provider
- CMS platform

Use cases:

- serious revenue-generating businesses
- stack-maturity filtering
- outreach personalisation

## Export Plan

## A. Keep and use existing files

Keep:

- all valid `Recently Added` files
- all valid `No Longer Detected` files

Exclude for now:

- the bad WooCommerce duplicate file

Re-download:

- WooCommerce `No Longer Detected`

## B. Download next

Download these into:

`/Users/laurencedeer/Desktop/BuiltWith/BuiltWith Exports/Current Detected/CMS and Ecommerce Platforms`

Priority order:

1. Shopify current detected
2. Shopify Plus current detected
3. WooCommerce Checkout current detected
4. BigCommerce current detected
5. Magento current detected
6. Magento Enterprise current detected if available
7. PrestaShop current detected
8. OpenCart current detected

All downloads should be filtered to:

- `AU`
- `NZ`
- `SG`

or exported broadly and then filtered locally by `Country`.

## Country-Specific Rule

For outreach and final working lists, split the data into:

- Australia
- New Zealand
- Singapore

Do not keep them only as one merged ANZ/SG blob.
You want:

- one combined master dataset
- plus separate country views

## Quality Control Rules

### Keep records when:

- `Country` is `AU`, `NZ`, or `SG`
- `Root Domain` is present
- platform signal is present

### Prioritise records when:

- company name is present
- phone or email is present
- people/contact fields are present
- payment platforms exist
- marketing automation or CRM exists
- hosting/provider data exists

### Down-rank records when:

- country is blank
- company is blank
- no contact details exist
- no useful stack detail exists

### Exclude records when:

- the file is known to be duplicated or wrong
- the domain is clearly invalid

## Presentation Model

The data should not be presented as one giant spreadsheet only.

It should be presented as:

### 1. `master_leads.csv`

One row per `root_domain`.

### 2. `platform_events.csv`

One row per:

- domain
- platform
- event type
- source file

Event type examples:

- `current_detected`
- `recently_added`
- `no_longer_detected`

### 3. `migration_pairs.csv`

One row per probable migration:

- domain
- old platform
- new platform
- supporting event dates

### 4. `contacts.csv`

One row per contact person / contact record where available.

### 5. `bucket_views`

These are the actual prospecting outputs.

Initial bucket views should be:

- `au_current_shopify.csv`
- `nz_current_shopify.csv`
- `sg_current_shopify.csv`
- `au_recent_migrations.csv`
- `nz_recent_migrations.csv`
- `sg_recent_migrations.csv`
- `au_high_value_stack_changes.csv`
- `nz_high_value_stack_changes.csv`
- `sg_high_value_stack_changes.csv`
- `anzsg_probable_migrations.csv`

## Priority Tiers

### Tier A

High-confidence, outreach-ready leads:

- AU/NZ/SG confirmed
- clear platform signal
- company present
- phone or email present
- commercial stack evidence present
- migration or switching trigger present

### Tier B

Good leads with weaker contactability or lower urgency:

- AU/NZ/SG confirmed
- clear platform signal
- some business detail
- less complete contacts or weaker stack evidence

### Tier C

Valid but low-confidence or long-tail leads:

- AU/NZ/SG confirmed
- platform detected
- poor contacts or sparse context

## What We Can Do Immediately

Before the missing WooCommerce file and current detected exports arrive, we can still:

1. Filter all existing valid files to `AU`, `NZ`, and `SG`
2. Remove the bad WooCommerce duplicate from analysis
3. Build `recently_added` and `no_longer_detected` event tables
4. Identify the first probable migration corridors from the current files
5. Build a provisional high-value shortlist using:
   - Shopify Plus recently added
   - BigCommerce recently added
   - BigCommerce no longer detected
   - Magento Enterprise no longer detected

## Next Download Checklist

When you go back into BuiltWith, the next downloads should be:

1. Correct WooCommerce `No Longer Detected`
2. Shopify `Current Detected`
3. Shopify Plus `Current Detected`
4. WooCommerce Checkout `Current Detected`
5. BigCommerce `Current Detected`
6. Magento `Current Detected`
7. Magento Enterprise `Current Detected`
8. PrestaShop `Current Detected`
9. OpenCart `Current Detected`

## Bottom Line

The formal plan is:

1. Restrict the entire project to `AU`, `NZ`, and `SG`
2. Use existing files for trigger-based prospecting now
3. Add `Current Detected` exports to create full market coverage
4. Merge everything at the `root_domain` level
5. Present the outcome as ranked country-specific lead buckets, not raw export dumps
