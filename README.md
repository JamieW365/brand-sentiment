# Brand Sentiment Analysis Platform

An end-to-end NLP platform for multi-dimensional brand sentiment analysis across Yelp, Reddit, app stores, and news sources.

Reviews are scored across three dimensions — sentiment, emotion, and aspect — and surfaced via a queryable API and interactive dashboard.

---

## Stack

| Layer | Technology |
|---|---|
| NLP | HuggingFace Transformers, MLFlow |
| Data Engineering | PySpark (AWS Glue), Apache Airflow (MWAA), S3, RDS Postgres |
| API | FastAPI, Redis |
| Frontend | Streamlit |
| Infrastructure | AWS (ECS Fargate, ECR), Terraform, GitHub Actions |
| Language | Python 3.12, Poetry |

---

## Data Flow (Phase 1)

```
Scraper (BaseScraper subclass)
    └── S3Writer → s3://bucket/raw/{source}/{YYYY-MM-DD}/data.jsonl
          └── PySpark ETL job (etl/jobs/yelp_reviews.py)
                ├── explicit raw schema → transform → validated rows
                └── idempotent insert (ON CONFLICT DO NOTHING) → Postgres `reviews`
```

Every stage is safe to re-run: the ETL skips reviews that already exist, so reprocessing a day never creates duplicates.

---

## Quick Start

> The `make` targets below are the planned developer interface and land with the Docker step of Phase 1. Until then, use the local development steps in the next section.

```bash
make up       # start all services
make ingest   # run ingestion scrapers
make score    # run NLP scoring pipeline
```

---

## Local Development

Prerequisites: Python 3.12, Poetry, Docker, and a JDK (17 recommended) for the PySpark tests.

Dependency groups are optional, so install the ones you need:

```bash
poetry install --with db,aws,ingestion,etl,dev
```

Copy `.env.example` to `.env` and fill in the values. All settings fields are required, but the migrations only use `DATABASE_URL`.

Start a local Postgres and apply the schema:

```bash
docker run -d --name brand-sentiment-db \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=brand_sentiment \
  -p 5432:5432 postgres:15
poetry run alembic upgrade head
poetry run alembic check        # confirm models and migrations are in sync
```

Run the checks:

```bash
poetry run ruff check .         # lint
poetry run pytest -v            # unit tests plus integration tests against local Postgres
```

Integration tests skip themselves when Postgres is unreachable. Set `REQUIRE_DB=1` to make an unreachable database a failure instead, as CI will.

Run the pipeline stages by hand:

```bash
# Live Yelp scrape to S3 (needs real Yelp and AWS credentials)
poetry run python -m ingestion.yelp_api.scraper

# Load one day of raw JSONL into Postgres (a local path works too)
poetry run python -m etl.jobs.yelp_reviews \
  --input-path s3://<bucket>/raw/yelp_api/2026-10-01/data.jsonl \
  --database-url postgresql://postgres:postgres@localhost:5432/brand_sentiment
```

---

## Documentation

For a full breakdown of architecture, design decisions, and component detail see [docs/architecture.md](docs/architecture.md).

---

## Build Status

| Phase | Scope | Status |
|---|---|---|
| P0 — Foundation | Repo setup, Poetry, shared package | ✅ Complete |
| P1 — Ingestion | Scrapers, S3, PySpark ETL, Airflow, Docker | 🔧 In progress |
| P2 — NLP | HuggingFace pipeline, MLFlow | ⏳ Pending |
| P3 — API | FastAPI, Redis | ⏳ Pending |
| P4 — Frontend | Streamlit dashboard | ⏳ Pending |
| P5 — Infrastructure | CI/CD, ECS deploy, Terraform | ⏳ Pending |

### Phase 1 breakdown

| Component | Status |
|---|---|
| Ingestion base layer (`BaseScraper`, S3 JSONL writer) | ✅ Complete |
| Yelp Fusion API scraper (with unit tests) | ✅ Complete |
| Reddit, App Store, News API scrapers | ⏳ Deferred until the pipeline runs end to end |
| Postgres schema (`Review` model, async Alembic migrations, model tests) | ✅ Complete |
| PySpark ETL (raw JSONL → `reviews`, idempotent writer, job entrypoint, tests) | ✅ Complete |
| Repo-wide linting (ruff) | ✅ Complete |
| Docker (per-service Dockerfiles, root `docker-compose.yml`, Makefile) | 🔧 Next |
| Airflow DAGs (`daily_ingest_dag`, `seed_data_dag`) | ⏳ Pending |
| Glue deployment of the ETL job (packaging, Secrets Manager) | ⏳ Phase 5 |
