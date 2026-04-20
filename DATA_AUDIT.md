# BuiltWith Data Audit

## Folder Inventory

Reviewed folder:

`/Users/laurencedeer/Desktop/BuiltWith`

Data-bearing subfolders:

- `BuiltWith Exports/Recently Added CMS`
- `BuiltWith Exports/No Longer Detected`

Non-data files:

- `BUILTWITH_SPRINT_PLAN.md`
- `BUILTWITH_DOWNLOAD_AND_MERGE_PLAN.md`
- `.DS_Store`
- one `.textClipping` file

## What Data You Currently Have

This is already a meaningful trigger dataset, not just raw prospecting exports.

You currently have two BuiltWith lead types:

1. `Recently Added CMS`
   These are domains where a target eCommerce/CMS technology appears to have been newly detected.

2. `No Longer Detected`
   These are domains where a target technology was previously detected and then disappeared.

That means the current dataset is strongest for:

- migration leads
- rebuild / relaunch leads
- switching-intent leads
- post-platform-change outreach

It is weaker for:

- broad firmographic list building across all verticals
- comprehensive contact enrichment
- non-trigger prospecting buckets

## Shared Schema

All CSVs reviewed use the same 42-column schema:

1. `Root Domain`
2. `Location on Site`
3. `Primary Domain`
4. `Technology Spend`
5. `Sales Revenue`
6. `Social`
7. `Employees`
8. `SKU`
9. `Company`
10. `Vertical`
11. `Tranco`
12. `Page Rank`
13. `Majestic`
14. `Umbrella`
15. `Telephones`
16. `Emails`
17. `X`
18. `Twitter`
19. `Facebook`
20. `LinkedIn`
21. `People`
22. `Verified Profiles`
23. `City`
24. `State`
25. `Zip`
26. `Country`
27. `First Detected`
28. `Last Found`
29. `First Indexed`
30. `Last Indexed`
31. `eCommerce Platform`
32. `CMS Platform`
33. `CRM Platform`
34. `Marketing Automation Platform`
35. `Payment Platforms`
36. `CRuX Rank`
37. `Cloudflare Rank`
38. `Agency`
39. `Hosting Provider`
40. `AI`
41. `Exclusion`
42. `Compliance`

## File-By-File Review

### Recently Added CMS

#### `BigCommerce websites that were added Recently.csv`

- Approx rows: 322
- Strongest fields: company, phone/email, hosting, payments, eCommerce platform
- Contactability is strong for this file
- Country concentration is mostly `US`
- Good fit for high-quality, lower-volume migration / adoption outreach

#### `Magento websites that were added Recently APEC.csv`

- Approx rows: 1,011
- Strong mix of company, vertical, location, and stack fields
- Contactability is moderate
- Large `US` bias despite the `APEC` naming
- Good fit for migration / rebuild trigger lists

#### `OpenCart websites that were added Asia-Pacific Economic Cooperation.csv`

- Approx rows: 2,426
- Data quality is weak
- Most rows have blank country, blank company, blank vertical
- Contactability is very low
- Still useful as a raw trigger file, but poor for direct outreach without further filtering

#### `PrestaShop websites that were added Recently APEC.csv`

- Approx rows: 423
- Small file with limited business/contact coverage
- Useful for niche migration buckets, but not a top-priority list

#### `Shopify Plus websites that were added RecentlyAPEC.csv`

- Approx rows: 2,579
- Highest-quality of the “recently added” files
- Strong company coverage
- Good eCommerce, payment, hosting, and marketing automation detail
- Decent email/phone coverage
- Good candidate for premium-priority outreach and segmentation

#### `WooCommerce Checkout websites that were added Recently.csv`

- Approx rows: 40,484
- Largest “recently added” dataset
- High volume, but much patchier business/contact coverage than Shopify Plus
- Good eCommerce and CMS fields
- Good hosting coverage
- Many blanks in country / vertical / company
- Best used as a scale list that gets narrowed hard by quality filters

#### `Shopify websites that were added RecentlyAPEC.textClipping`

- This is not a lead export
- It contains plain text: `Asia-Pacific_Economic_Cooperation_13_-_2026-04-13`
- Most likely a saved note or copied filter label
- No prospecting value by itself

### No Longer Detected

#### `BigCommerce websites that is no longer detectedAPEC.csv`

- Approx rows: 52,172
- Strong business/contact coverage for a large file
- Strong location coverage
- Good fit for platform-switch / churn / migration outreach

#### `Magento Enterprise websites that is no longer detectedAPEC.csv`

- Approx rows: 13,859
- High quality relative to size
- Best `People` coverage in the current dataset
- Good stack detail and strong location coverage
- High-value list for enterprise migration or post-change offers

#### `OpenCart websites that is no longer detectedAPEC.csv`

- Approx rows: 206,660
- Largest file in the dataset
- Very weak data quality
- Huge blank rates for country, company, vertical, and contacts
- Useful for broad signal mining, not for immediate outreach without filtering

#### `PrestaShop websites that is no longer detectedAPEC.csv`

- Approx rows: 58,149
- Medium quality
- Better than OpenCart, weaker than BigCommerce / Magento Enterprise
- Good for secondary migration buckets

#### `Woocommrce Cehcout sited no longer detected APEC.csv`

- Approx rows: 52,172
- This file is an exact duplicate of the BigCommerce “no longer detected” file
- Same sample rows
- Same fill rates
- Same SHA-1 hash
- This should not be trusted as a unique WooCommerce export

## Important Data Quality Findings

### 1. One file is a duplicate / mislabeled export

`Woocommrce Cehcout sited no longer detected APEC.csv` is byte-for-byte identical to:

`BigCommerce websites that is no longer detectedAPEC.csv`

Practical implication:

- treat the WooCommerce no-longer-detected file as bad input
- exclude it from merge logic until replaced

### 2. The “APEC” label does not mean clean APEC-only geography

Several files are dominated by `US` rows, which is acceptable because the US is in APEC.
But some files also include many blank countries and some apparently non-APEC geographies like `RU`.

Practical implication:

- never trust folder names as geography truth
- always filter using the `Country` field after export

### 3. Data quality varies heavily by platform

Best-quality files:

- Shopify Plus recently added
- BigCommerce recently added
- Magento Enterprise no longer detected
- BigCommerce no longer detected

Weakest files:

- OpenCart recently added
- OpenCart no longer detected
- PrestaShop recently added

Practical implication:

- treat the stronger files as outreach-ready with light cleanup
- treat weaker files as signal pools that need strict filtering

### 4. Timing fields are present in every file and are highly valuable

Every export contains:

- `First Detected`
- `Last Found`
- `First Indexed`
- `Last Indexed`

These are the best fields in the current dataset because they support:

- recent adoption buckets
- recently dropped / switched buckets
- migration timing logic
- urgency scoring

### 5. Contacts exist, but unevenly

Contact fields include:

- telephones
- emails
- people
- social links

Strongest contactability:

- BigCommerce no longer detected
- Magento Enterprise no longer detected
- BigCommerce recently added
- Shopify Plus recently added

Weak contactability:

- OpenCart both folders
- PrestaShop recently added
- WooCommerce recently added

### 6. Stack fields are richer than a normal contact export

The current files include directly useful stack columns:

- eCommerce platform
- CMS platform
- CRM platform
- marketing automation platform
- payment platforms
- hosting provider
- AI

This means the current exports are already good enough to support:

- stack-based segmentation
- platform-specific hooks
- migration / rebuild messaging
- partial fingerprinting without a separate custom export

## Coverage Summary

Approximate company-level quality by file:

- `Shopify Plus recently added`: excellent
- `BigCommerce recently added`: strong
- `Magento recently added`: solid
- `BigCommerce no longer detected`: strong
- `Magento Enterprise no longer detected`: strong
- `PrestaShop no longer detected`: medium
- `WooCommerce recently added`: medium-to-weak at volume
- `OpenCart recently added`: weak
- `OpenCart no longer detected`: weak

Approximate outreach readiness by file:

- immediate outreach candidates: Shopify Plus added, BigCommerce added, BigCommerce removed, Magento Enterprise removed
- needs filtering first: Magento added, PrestaShop removed, WooCommerce added
- needs heavy cleanup first: OpenCart added, OpenCart removed, PrestaShop added

## Best Immediate Uses Of This Dataset

With only the current files, the best prospecting plays are:

### 1. Migration leads

Use:

- all `Recently Added CMS` files
- all `No Longer Detected` files except the duplicate WooCommerce file

Best hook:

- “We help stabilise traffic after platform changes.”

### 2. Churn / switch-intent lists

Use:

- `No Longer Detected` exports

Prioritise:

- BigCommerce no longer detected
- Magento Enterprise no longer detected

### 3. High-value platform slices

Use:

- Shopify Plus recently added
- Magento Enterprise no longer detected
- BigCommerce recently added / removed

Filter by:

- company present
- phone or email present
- country present

### 4. Stack-personalised outreach

Use fields like:

- payment platforms
- hosting provider
- CRM platform
- marketing automation platform

This lets you tailor messaging without new exports.

## What Is Missing Right Now

Compared with your full brief, the current folder does not yet include:

- BuiltWith custom stack exports
- live technology long-format exports
- CRM format export separated by people
- domain attributes export
- related domains / related attributes export
- matrix export
- postal export

Practical implication:

You have a strong starting trigger dataset, but not yet the full reusable BuiltWith lead library described in the brief.

## Recommended Merge Approach For Current Files

For the current folder specifically:

1. Exclude the duplicate WooCommerce no-longer-detected file
2. Standardise all column names
3. Canonicalise to `root_domain`
4. Add a `source_type` field:
   - `recently_added`
   - `no_longer_detected`
5. Add a `technology_bucket` field:
   - `bigcommerce`
   - `magento`
   - `magento_enterprise`
   - `opencart`
   - `prestashop`
   - `shopify_plus`
   - `woocommerce_checkout`
6. Add `quality_tier` based on data completeness
7. Build one merged master table from the valid CSVs
8. Keep a separate issue log for weak/mislabeled files

## Bottom Line

You already have a useful trigger-led lead base.

The strongest insight from the review is:

- this dataset is good for migration and switching outreach right now
- data quality is uneven, so platform-level prioritisation matters
- one file is definitely duplicated and should be removed from analysis
- geography labels cannot be trusted without checking `Country`
- timing fields are the most valuable common signal across all files
