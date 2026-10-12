# TerraSignal

Geospatial machine intelligence platform. Built with FastAPI, SQLAlchemy, JWT auth, SQLite for local development, Leaflet maps, and Lucide icons.

## Overview

TerraSignal is a production-oriented geospatial intelligence application with:

- Responsive public site and dashboard shell with Lucide icons and Leaflet map tiles.
- Email/password registration and login; password hashing; signed access tokens.
- Server-side role checks for `admin`, `researcher`, `analyst`, and `organization`.
- Protected `/api/me`, `/api/overview`, `/api/alerts`, `/api/admin/users` endpoints.
- Persistent user accounts, alert records, and audit events through SQLAlchemy.
- Analyst-facing filters, map markers, alerts list, dashboard navigation, login/register forms, logout, CSV export, and an admin user view.
- Extensive domain, service, pipeline, ML, geo, and utility modules for signal processing, enrichment, scoring, classification, feature stores, spatial operations, and more.
- Pytest API tests for auth, authorization, and overview.
- Dockerfile and docker-compose for local startup.

## Project layout

```
app/
  main.py              # FastAPI application entrypoint
  models.py, schemas.py, security.py, database.py, ...
  services/            # Business services (alerts, notifications, metrics, ...)
  core/                # Events, registry, middleware, permissions, ...
  domain/              # Domain models and value objects
  pipelines/           # Ingest, enrich, score, classify, persist, ...
  ml/                  # Feature store, predictor, drift detection, ...
  geo/                 # Projection, distance, geojson, tiling, ...
  utils/               # Time, stats, hashing, and shared helpers
  api/                 # Route modules and API helpers
  static/              # Frontend (HTML/JS/CSS)
tests/
requirements.txt
requirements.lock      # Pinned dependency versions
example.env            # Environment template (copy to .env)
```

## Run locally

Requires Python 3.11+.

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
source .venv/bin/activate
pip install -r requirements.txt
# Or with Poetry (uses pyproject.toml + poetry.lock):
#   poetry install
cp example.env .env
uvicorn app.main:app --reload
```

Open http://127.0.0.1:8000. API docs: http://127.0.0.1:8000/docs

First registered account receives `analyst`. For a local admin, set `BOOTSTRAP_ADMIN_EMAIL=admin@example.com` in `.env` before registering that address. Never use demo secrets in production.

## Tests

```bash
pytest -q
```

## Docker

```bash
docker compose up --build
```

## Roles

- `analyst`: overview, map, alerts, export.
- `researcher`: analyst features plus research navigation placeholder.
- `organization`: analyst features plus organization workspace placeholder.
- `admin`: all features plus user management.

## Production notes

- Replace SQLite with managed PostgreSQL/PostGIS; set a long random `SECRET_KEY`; enforce HTTPS.
- Add Alembic migrations, email verification, MFA/SSO, rate limiting, and secrets management as needed.
- Wire real satellite/environmental data providers and inference services; current alert data is seeded demo data.
- Map tiles are served by OpenStreetMap and require attribution.
