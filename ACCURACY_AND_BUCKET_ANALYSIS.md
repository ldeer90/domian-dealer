# Accuracy And Bucket Analysis

## Current Scope

This analysis reflects the current AU / NZ / SG processed dataset after adding the real WooCommerce removal export.

Current processed totals:

- `150,092` unique leads
- `153,154` platform events implied by the current source mix
- `384` migration pairs

## Biggest Change

The missing WooCommerce removal file changed the shape of the project dramatically.

It added:

- `132,041` WooCommerce removal leads in AU / NZ / SG

That means WooCommerce removals are now the dominant signal in the database.

## Accuracy Findings

## 1. Geography is still mostly usable, but review matters more at scale

Geo confidence split:

- `tld_match`: strong match between domain TLD and country
- `country_only`: acceptable country match but no TLD confirmation
- `tld_mismatch`: TLD suggests a different country

Current mismatch count:

- `643`

Recommendation:

- use `geo_review_needed` as a visible review bucket
- do not delete these automatically

## 2. “Migration” still needs to be split by recency

Migration gap bins:

- `<= 90 days`: `6`
- `91 to 180 days`: `49`
- `181 to 365 days`: `47`
- `366 to 1095 days`: `114`
- `> 1095 days`: `154`
- `negative`: `14`

Interpretation:

- true recent migration signals exist, but they are still a minority
- most migration pairs are better treated as historical replatform signals
- negative gaps should be treated as overlap/noise, not clean migration proof

## 3. WooCommerce removals are real, but the raw bucket is too broad

`woocommerce_removed_signal` count:

- `132,041`

Tier mix inside this bucket:

- A: `473`
- B: `11,138`
- C: `39,946`
- D: `80,484`

Interpretation:

- the raw WooCommerce removal pool is useful as a database
- but it is too broad to use as a frontline outreach list without more filtering

## 4. WooCommerce becomes powerful when narrowed

The strongest new Woo-specific buckets are:

- `woo_removed_revenue_stack`: `5,849`
- `woo_to_shopify`: `124`
- `woo_to_shopify_recent`: `76`
- `woo_to_shopify_plus`: `41`
- `woo_to_shopify_plus_recent`: `13`

These are much better outbound slices than plain Woo removals.

## Scoring Rebalance

The scoring model has been reweighted so raw removal signals no longer dominate.

Current logic now does this:

- recent additions score higher than removals
- matched migration windows get meaningful bonus
- removed-only records get much less trigger weight
- contactability and stack maturity now matter more for surfacing leads

Current tier distribution after rebalancing:

- Tier A: `960`
- Tier B: `15,497`
- Tier C: `46,488`
- Tier D: `87,147`

Interpretation:

- the model now acts more like a sales shortlist generator than a signal hoarder
- broad low-confidence removal pools are preserved, but they no longer crowd the top

## Current Sales Buckets

These are now generated directly into the lead dataset and exposed in the UI.

### Broad signal buckets

- `platform_removed_signal`: `146,689`
- `recent_platform_adopter`: `3,806`
- `recent_migration_signal`: `115`
- `historic_replatform_signal`: `245`
- `geo_review_needed`: `643`

### Commercial maturity buckets

- `revenue_stack`: `8,757`
- `marketing_mature`: `15,325`
- `contactable_revenue_stack`: `7,761`
- `contact_ready_ab`: `16,140`
- `premium_hosting_ab`: `8,318`
- `high_spend`: `6,882`
- `shopify_plus_target`: `330`

### WooCommerce-specific buckets

- `woocommerce_removed_signal`: `132,041`
- `woo_removed_revenue_stack`: `5,849`
- `woo_to_shopify`: `124`
- `woo_to_shopify_recent`: `76`
- `woo_to_shopify_plus`: `41`
- `woo_to_shopify_plus_recent`: `13`

### Platform-switch buckets

- `switch_to_shopify`: `315`
- `switch_to_woocommerce`: `71`

## Best New Sales Slices

The strongest practical lists now are:

### 1. `recent_migration_signal`

- `115` leads
- mostly A/B quality
- strongest urgency bucket

Tier mix:

- A: `80`
- B: `33`
- C: `2`

### 2. `contactable_revenue_stack`

- `7,761` leads
- broad but commercially useful
- ideal for general outbound campaigns

Tier mix:

- A: `683`
- B: `5,929`
- C: `1,149`

### 3. `woo_removed_revenue_stack`

- `5,849` leads
- strongest scalable WooCommerce sub-bucket

Tier mix:

- A: `340`
- B: `4,519`
- C: `990`

### 4. `woo_to_shopify` + `woo_to_shopify_recent`

Combined:

- `200` leads

Tier mix:

- A: `150`
- B: `92`
- C: `10`

This is one of the best new corridor-led buckets in the project.

### 5. `woo_to_shopify_plus` + `woo_to_shopify_plus_recent`

Combined:

- `54` leads

Tier mix:

- A: `41`
- B: `13`

This is small, but premium and likely very strong for tailored outreach.

## Key WooCommerce Insights

Top Woo removal corridor destinations:

- `shopify`: `200`
- `shopify_plus`: `54`
- `woocommerce_checkout`: `40`
- `magento`: `5`

Interpretation:

- Shopify is now clearly the main visible destination from WooCommerce in your current trigger set
- Shopify Plus is small in count but high in quality

Useful WooCommerce sub-slices:

- Woo removed + contactable: `87,723`
- Woo removed + revenue stack: `6,546`
- Woo removed + contactable + revenue stack: `5,849`
- Woo removed + premium hosting + contactable + revenue stack: `4,481`
- Woo removed + new platform + contact: `208`
- Woo removed + new platform + revenue stack: `70`

## What To Prioritise In The UI

The best bucket combinations to save as views are:

- `recent_migration_signal` + `contact_ready_ab`
- `contactable_revenue_stack` + `high_spend`
- `woo_removed_revenue_stack` + `premium_hosting_ab`
- `woo_to_shopify` + `contact_ready_ab`
- `woo_to_shopify_plus` + `high_spend`
- `switch_to_shopify` + `marketing_mature`

## Bottom Line

The WooCommerce file made the database much more valuable, but only after narrowing.

What changed in practical terms:

- the database became much larger
- the broad removal pool became less useful by itself
- the best new buckets are now WooCommerce-to-Shopify and WooCommerce revenue-stack slices
- the scoring model is now better aligned to sales usefulness than raw signal volume

## Shopify Current-State Buckets

After adding the current detected Shopify and Shopify Plus exports, the following current-state buckets are now available in the UI:

- `current_shopify`: `127,495`
- `current_shopify_contactable`: `81,185`
- `current_shopify_revenue_stack`: `34,444`
- `current_shopify_high_spend`: `11,877`
- `current_shopify_removed_woocommerce`: `10,351`
- `current_shopify_plus`: `5,160`
- `current_shopify_plus_contactable`: `3,967`
- `current_shopify_plus_high_spend`: `4,458`
- `current_shopify_plus_removed_woocommerce`: `495`

## Best Shopify Current-State Sales Buckets

### `current_shopify_revenue_stack`

- `34,444` leads
- strongest broad Shopify commercial bucket

Tier mix:

- A: `668`
- B: `14,775`
- C: `18,997`
- D: `4`

### `current_shopify_contactable`

- `81,185` leads
- broad operational bucket for cold outreach

### `current_shopify_removed_woocommerce`

- `10,351` leads
- highly useful “now on Shopify, previously on Woo” bucket

Tier mix:

- A: `328`
- B: `5,504`
- C: `4,021`
- D: `498`

### `current_shopify_plus_contactable`

- `3,967` leads
- strong premium outreach segment

Tier mix:

- A: `235`
- B: `2,086`
- C: `1,582`
- D: `64`

### `current_shopify_plus_high_spend`

- `4,458` leads
- one of the best current-state premium commercial slices

Tier mix:

- A: `206`
- B: `1,904`
- C: `1,869`
- D: `479`
