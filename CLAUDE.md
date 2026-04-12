# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Spatial Clustering & Multimodal Urban Traffic Monitoring** — A real-time traffic incident detection and clustering system focused on Hà Nội, Vietnam. It ingests Vietnamese traffic news via RSS, extracts incident locations using PhoBERT (Vietnamese NER), clusters spatiotemporal incidents via ST-DBSCAN, and predicts zone congestion risk. Stores all findings in a PostGIS + TimescaleDB database.

## Environment Setup

```bash
# Activate virtual environment (Windows)
source .venv/Scripts/activate

# Install dependencies
pip install -r requirements.txt

# Start database (Docker)
docker-compose up -d

# Copy and configure environment
cp .env.example .env
```

**Required**: PostgreSQL 15+ with PostGIS and TimescaleDB extensions. Default DB: `traffic_monitoring` on `localhost:5432` (credentials in `.env`).

## Running the System

```bash
# Import geographic data (one-time setup)
python src/scripts/import_geojson.py    # Hà Nội administrative zones
python src/scripts/import_roads.py      # Road network geometry
python src/simulation/mock_events.py    # Insert test planned events

# Start the main scheduler (continuous operation, 24/7)
python src/main_scheduler.py

# On-demand zone prediction (zone name, hours ahead: 1/2/3)
python src/processing/zone_predictor.py "Láng" 1
```

No formal test suite. Modules can be run independently for development/testing (each has `if __name__ == "__main__"` guards).

## Architecture & Data Flow

```
RSS Feeds (8 Vietnamese sources)
  → news_scraper.py       [every 15 min] → raw_feed table
  → nlp_engine.py         [every 1 min]  → incident + location tables
  → st_dbscan.py          [every 10 min] → incident_cluster table
  → zone_predictor.py     [on-demand]    → risk score output
```

**Orchestration**: `src/main_scheduler.py` is the single entry point. It calls `compute_zone_metrics()` once at startup, then schedules three APScheduler jobs.

**Key module responsibilities:**

| Module | Role |
|--------|------|
| `src/core/config.py` | Load `.env` settings (DB credentials, paths) |
| `src/core/db_manager.py` | Connection pool (1–10 connections) via psycopg2 |
| `src/ingestion/news_scraper.py` | Fetch RSS feeds, deduplicate by MD5 hash + Jaccard similarity, filter by traffic keywords |
| `src/processing/nlp_engine.py` | PhoBERT NER → entity extraction → geocoding → `potential_score` calculation |
| `src/processing/event_analyzer.py` | Boost incident scores for nearby planned events (70% distance + 30% attendance) |
| `src/processing/st_dbscan.py` | Spatiotemporal DBSCAN: 500m radius, 2-hour window, Haversine distance |
| `src/processing/zone_metrics.py` | Infrastructure pressure score per zone (60% pop density + 40% road shortage) |
| `src/processing/zone_predictor.py` | Congestion risk = 0.4×pressure + 0.4×incidents + 0.2×events |
| `src/utils/geo_helpers.py` | Nominatim geocoding fallback (rate-limited, no API key) |
| `src/utils/text_helpers.py` | Text normalization, content hashing, Jaccard similarity |

## Database Schema

Tables are created programmatically with `IF NOT EXISTS` — no migration framework. Key tables:

- **`raw_feed`** — raw RSS articles with `content_hash` index for deduplication
- **`incident`** — extracted incidents with `potential_score` (0.0–1.0), `confidence_level`, FK to `location` and `incident_cluster`
- **`incident_cluster`** — ST-DBSCAN results: centroid, `avg_severity`, `zone_name`, `eps1_meters`, `eps2_hours`
- **`administrative_zone`** — Hà Nội wards with PostGIS `MULTIPOLYGON`, `zone_pressure_score`
- **`road_network`** — road geometry with PostGIS GIST index
- **`planned_event`** — scheduled events (concerts, sports) with `impact_radius_meters`
- **`incident_category`** / **`traffic_keyword`** — lookup tables for scoring (loaded into memory at NLP startup via `load_lexicon_from_db()`)

## Key Design Decisions

- **Vietnamese NLP**: Uses `vinai/phobert-base` from HuggingFace with a token-classification NER pipeline. Falls back to regex patterns if PhoBERT confidence is low.
- **Deduplication**: Two-stage — exact MD5 hash on content, then Jaccard similarity threshold (configurable) on near-duplicates.
- **ST-DBSCAN**: Custom pure-Python implementation (no sklearn dependency) in `st_dbscan.py`. Parameters `eps1` (meters) and `eps2` (hours) are stored per cluster for auditability.
- **Scoring pipeline**: `potential_score` is built incrementally in `nlp_engine.py` — base category score → keyword multipliers → event bonus → infrastructure density adjustment.
- **All logging is in Vietnamese** to stdout (stdout reconfigured to UTF-8 in `main_scheduler.py`).
- **No file-based logs** — all output goes to stdout only.
