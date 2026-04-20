# Current Data Analysis

## Scope

This analysis uses the current valid BuiltWith trigger exports only and filters to:

- `AU`
- `NZ`
- `SG`

Excluded from processing:

- `Woocommrce Cehcout sited no longer detected APEC.csv`

Reason:

- it is a confirmed duplicate of the BigCommerce no-longer-detected export

## Processed Output

The current pipeline now produces:

- [leads.csv](/Users/laurencedeer/Desktop/BuiltWith/processed/leads.csv)
- [platform_events.csv](/Users/laurencedeer/Desktop/BuiltWith/processed/platform_events.csv)
- [migration_pairs.csv](/Users/laurencedeer/Desktop/BuiltWith/processed/migration_pairs.csv)
- [summary.json](/Users/laurencedeer/Desktop/BuiltWith/processed/summary.json)
- [builtwith.db](/Users/laurencedeer/Desktop/BuiltWith/processed/builtwith.db)

## Real Size Of The Current AU/NZ/SG Dataset

- `20,641` unique leads
- `21,113` platform events
- `122` probable migration pairs

Country split:

- Australia: `16,607`
- Singapore: `2,570`
- New Zealand: `1,464`

## What The Current Data Is Best At

The current folder is best for:

- migration detection
- switching-intent detection
- recent platform adoption signals
- stack-aware ecommerce prospecting

It is not yet a full current-platform install base.

## Event Coverage

Recently added platforms:

- `woocommerce_checkout`: `2,059`
- `shopify`: `1,348`
- `shopify_plus`: `330`
- `magento`: `48`
- `opencart`: `25`
- `bigcommerce`: `14`
- `prestashop`: `12`

Removed platforms:

- `opencart`: `8,676`
- `bigcommerce`: `4,384`
- `prestashop`: `3,081`
- `magento_enterprise`: `1,136`

## Migration Signal Strength

Domains with both an added and removed platform signal:

- `117`

Derived migration pairs:

- `122`

Top migration corridors in the current files:

- `opencart -> shopify`: `21`
- `opencart -> woocommerce_checkout`: `19`
- `prestashop -> shopify`: `15`
- `bigcommerce -> shopify`: `12`
- `opencart -> shopify_plus`: `8`
- `magento_enterprise -> shopify`: `8`
- `prestashop -> shopify_plus`: `8`
- `bigcommerce -> woocommerce_checkout`: `7`
- `prestashop -> woocommerce_checkout`: `7`
- `magento_enterprise -> shopify_plus`: `6`

## Lead Quality Distribution

Priority tiers from the current scoring model:

- Tier A: `2,015`
- Tier B: `6,220`
- Tier C: `8,787`
- Tier D: `3,619`

This means the current dataset already contains a sizeable outreach-ready slice before the missing downloads arrive.

## Best Immediate Lead Types

The strongest immediate lead classes are:

### 1. High-confidence migration candidates

These are domains with both:

- a removed platform
- a newly added platform

This is the most commercially useful subset.

### 2. Premium replatforming signals

Especially:

- `magento_enterprise -> shopify`
- `magento_enterprise -> shopify_plus`
- `bigcommerce -> shopify`
- `bigcommerce -> shopify_plus`

### 3. High-volume lower-cost migration pools

Especially:

- `opencart -> shopify`
- `opencart -> woocommerce_checkout`
- `prestashop -> shopify`

### 4. App-led / revenue-stack leads

These are leads with evidence of:

- payment platforms
- CRM platforms
- marketing automation tools
- mature hosting

These should be prioritised inside each trigger bucket.

## Best Current Examples

Some of the strongest A-tier examples currently surfaced by the processed model include:

- `wittner.com.au`
- `williecreekpearls.com.au`
- `enjo.com.au`
- `wotnot.com.au`
- `livingstone.com.au`
- `healthpost.co.nz`
- `all4kidsonline.com.au`

These are useful for validating outreach hooks and UI views.

## What The Missing Downloads Will Improve Later

When you add the remaining `Current Detected` exports and the corrected WooCommerce removal export, the system will improve in three ways:

1. better current install-base coverage
2. better migration confirmation
3. broader TAM-style prospecting beyond trigger-only lists

## Bottom Line

The current data is already strong enough to justify building the UI now.

Not because it is complete, but because it already supports:

- meaningful search and filtering
- migration corridor analysis
- country-specific campaign slicing
- stack-aware prospect prioritisation
