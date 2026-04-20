# DOMAIN DEALER

## What This Includes

- a processing pipeline for the current BuiltWith exports
- AU / NZ / Singapore-only filtering
- processed CSV outputs
- a SQLite database for the local app
- a lightweight FastAPI backend
- a React + Vite frontend for searching and filtering leads

## Project Paths

### Raw exports

- `/Users/laurencedeer/Desktop/BuiltWith/BuiltWith Exports`

### Processed outputs

- `/Users/laurencedeer/Desktop/BuiltWith/processed`

### Backend

- `/Users/laurencedeer/Desktop/BuiltWith/backend`

### Frontend

- `/Users/laurencedeer/Desktop/BuiltWith/frontend`

### Processing script

- [/Users/laurencedeer/Desktop/BuiltWith/tools/process_builtwith.py](/Users/laurencedeer/Desktop/BuiltWith/tools/process_builtwith.py)

## First-Time Setup

### Python environment

```bash
cd /Users/laurencedeer/Desktop/BuiltWith
python3 -m venv .venv
source .venv/bin/activate
pip install -r backend/requirements.txt
```

### Environment

Create a local env file and add your SE Ranking key if you want the migration outcome analysis:

```bash
cd /Users/laurencedeer/Desktop/BuiltWith
cp .env.example .env
export SERANKING_API_KEY=your-se-ranking-api-key
```

For the Vite frontend, create a frontend env file when you want the app to talk to a hosted backend:

```bash
cd /Users/laurencedeer/Desktop/BuiltWith/frontend
cp .env.example .env
```

### Frontend dependencies

```bash
cd /Users/laurencedeer/Desktop/BuiltWith/frontend
npm install
```

## Rebuild The Processed Data

Run this whenever new CSV exports are added:

```bash
cd /Users/laurencedeer/Desktop/BuiltWith
python3 tools/process_builtwith.py
```

This regenerates:

- `processed/leads.csv`
- `processed/platform_events.csv`
- `processed/migration_pairs.csv`
- `processed/summary.json`
- `processed/builtwith.db`

## Run The Backend

```bash
cd /Users/laurencedeer/Desktop/BuiltWith
source .venv/bin/activate
uvicorn backend.main:app --host 127.0.0.1 --port 8765
```

Backend URL:

- `http://127.0.0.1:8765`

API root:

- `http://127.0.0.1:8765/api`

## Run The Frontend

```bash
cd /Users/laurencedeer/Desktop/BuiltWith/frontend
npm run dev
```

Frontend URL:

- `http://127.0.0.1:5173`

The frontend reads its API base from `VITE_API_BASE`. If unset, it falls back to the local backend:

- `http://127.0.0.1:8765/api`

## Build The Frontend

```bash
cd /Users/laurencedeer/Desktop/BuiltWith/frontend
npm run build
```

## Deploy The Frontend To Vercel

This repo includes a root `vercel.json` that builds the Vite app from the `frontend` folder.

In Vercel, set:

- project root: repo root
- framework: Vite or auto-detect
- environment variable: `VITE_API_BASE=https://your-backend-url/api`

Important:

- Vercel will only host the frontend from this repo
- the FastAPI backend still needs to run elsewhere
- the frontend will not work correctly in production unless `VITE_API_BASE` points at that hosted backend

## Current App Features

- search by domain, company, or stack terms
- filter by country
- filter by tier
- filter by added platform
- filter by removed platform
- filter by vertical
- filter by migration-only leads
- filter by contact, CRM, marketing, and payment presence
- inspect lead detail
- inspect platform events
- inspect migration pairs
- export the current filtered view as CSV

## Important Notes

- the current system excludes the known bad duplicate WooCommerce removal export
- the current data is trigger-led, not yet a full current install-base
- when new BuiltWith exports arrive, rerun the processing script and the UI will pick them up from the rebuilt SQLite database
- the large processed datasets and SQLite databases are intentionally not committed to GitHub; rebuild them locally from the raw exports
