# Local UI Plan

## Goal

Build a lightweight locally hosted interface so you can:

- search domains and companies quickly
- filter by country, platform, trigger type, and stack
- review likely migration leads
- organise lead buckets for outreach
- mark lead status and campaign notes

## Product Shape

This should not be a heavy SaaS-style app.
It should be a fast local operations dashboard for lead review and campaign management.

## Core Requirements

The UI should let you:

1. Search by domain, company, email, person, or tool name
2. Filter by:
   - country
   - platform
   - event type
   - migration corridor
   - vertical
   - contactability
   - app / stack maturity
   - priority tier
3. Save views for outreach campaigns
4. Open a lead and see:
   - company info
   - stack info
   - trigger history
   - contact details
   - notes / status
5. Export filtered lists for outreach

## Recommended Architecture

Use a simple three-part local stack:

### 1. Database

`SQLite`

Why:

- local
- simple
- fast enough for this size
- easy to back up
- supports full-text search with FTS5

This is a better fit than trying to load giant CSVs directly in the browser.

### 2. Backend

`FastAPI`

Why:

- lightweight
- easy CSV import scripts
- fast local API
- easy to serve on localhost
- simple to extend later

### 3. Frontend

`React + Vite`

Why:

- fast local development
- strong table/filter UX
- easy saved filters and lead detail panels
- lightweight enough for this project

## Why Not Just Use A Spreadsheet

Because your dataset will become too large and too relational.

You need:

- one company table
- one event table
- one migration table
- one contact table
- one campaign/status layer

A frontend becomes useful once you need:

- multi-filter searching
- saved segments
- lead notes
- lead status tracking
- fast domain lookup

## Data Model For The UI

## Main tables

### `leads`

One row per `root_domain`.

Core columns:

- `id`
- `root_domain`
- `company`
- `country`
- `state`
- `city`
- `vertical`
- `tech_spend`
- `employees`
- `contact_score`
- `stack_score`
- `trigger_score`
- `priority_tier`
- `current_platforms`
- `payments`
- `crm_platforms`
- `marketing_platforms`
- `hosting_providers`
- `notes_count`
- `campaign_status`
- `last_updated_at`

### `platform_events`

One row per platform event.

Core columns:

- `id`
- `root_domain`
- `platform_name`
- `event_type`
- `first_detected`
- `last_found`
- `source_file`

Event types:

- `current_detected`
- `recently_added`
- `no_longer_detected`

### `migration_pairs`

One row per likely migration.

Core columns:

- `id`
- `root_domain`
- `old_platform`
- `new_platform`
- `confidence_level`
- `first_new_detected`
- `last_old_found`

### `contacts`

One row per contact record.

Core columns:

- `id`
- `root_domain`
- `person_name`
- `title`
- `email`
- `phone`
- `linkedin_url`

### `campaign_notes`

One row per internal note or action.

Core columns:

- `id`
- `root_domain`
- `status`
- `owner`
- `last_contacted_at`
- `next_action_at`
- `note`

### `saved_views`

Saved filter sets for repeated prospecting workflows.

Core columns:

- `id`
- `name`
- `filters_json`
- `created_at`

## Recommended UI Screens

### 1. Dashboard

Show:

- total leads in AU / NZ / SG
- leads by country
- leads by platform
- leads by event type
- top migration corridors
- high-contactability counts
- leads needing follow-up

### 2. Lead Explorer

This is the main working screen.

Features:

- search bar
- multi-filter sidebar
- sortable table
- bulk select
- export filtered CSV
- save current view

Suggested default columns:

- domain
- company
- country
- current platform
- event summary
- migration corridor
- stack score
- contact score
- priority tier
- outreach status

### 3. Lead Detail Panel

Open when a row is clicked.

Show:

- company and geo info
- all platform events
- likely migration history
- app/tool stack summary
- contacts
- notes
- outreach status

### 4. Campaign Views

Prebuilt tabs like:

- AU recent migrations
- NZ current Shopify
- SG Shopify Plus
- high-value stack changes
- no-contact leads
- follow-up due

### 5. Import / Refresh Screen

Simple local admin screen that shows:

- imported files
- row counts
- duplicates detected
- bad files flagged
- last refresh time

## Filters The UI Should Support

Minimum filter set:

- country
- platform
- event type
- old platform
- new platform
- vertical
- has email
- has phone
- has people/contact
- has payments
- has CRM
- has marketing automation
- has premium hosting
- priority tier
- campaign status

## Search Capabilities

Support search across:

- root domain
- company name
- email
- person name
- tool/platform name

Use SQLite FTS for:

- company
- domain
- contacts
- stack fields

## Presentation Principles

The UI should feel like an analyst console, not a CRM clone.

That means:

- dense but readable tables
- strong filter controls
- fast response
- little decorative clutter
- quick copy/export actions

## Suggested Design Direction

Local, practical, and campaign-focused.

Use:

- warm neutral background
- clear data table layout
- compact filters
- color only for status and tier emphasis

Avoid:

- oversized cards
- dashboard fluff
- heavy animations
- visual complexity that slows review

## MVP Scope

The first version should only do these things:

1. Import processed CSVs into SQLite
2. Search and filter leads
3. Open lead details
4. Save notes and outreach status
5. Export the current filtered list

That is enough to make the data operational.

## Nice-To-Have Later

After the MVP:

- saved campaigns
- duplicate resolution UI
- side-by-side migration compare view
- email template snippets by bucket
- reminder queue for follow-ups
- simple charts for lead distribution

## Local Hosting Plan

Run locally on:

- backend: `http://localhost:8000`
- frontend: `http://localhost:5173`

Or serve the built frontend through FastAPI later so you only open one local URL.

## Recommended Build Order

### Phase 1

- define processed CSV outputs
- define SQLite schema
- build import script

### Phase 2

- build backend API
- add search, filter, and export endpoints

### Phase 3

- build lead explorer UI
- build detail drawer
- build notes/status editing

### Phase 4

- add saved views
- add dashboard metrics
- add import health screen

## Why This Fits Your Brief

Your brief is really about creating many prospecting buckets from overlapping technographic signals.

This UI supports exactly that by making it easy to:

- browse the full market
- isolate trigger-based slices
- combine trigger + stack + geography filters
- manage outreach against those slices

## Recommendation

The best lightweight local product is:

- `SQLite`
- `FastAPI`
- `React + Vite`

It stays small, performs well locally, and is strong enough to handle the dataset you’re building without turning into an overbuilt project.
