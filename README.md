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

## Quick Start

> The `make` targets below are the planned developer interface and land with the Docker step of Phase 1. Until then, use the local development steps in the next section.

```bash
make up       # start all services
make ingest   # run ingestion scrapers
make score    # run NLP scoring pipeline
```

---

## Local Development

Dependency groups are optional, so install the ones you need:

```bash
poetry install --with db,aws,ingestion,dev
```

Start a local Postgres and apply the schema:

```bash
docker run -d --name brand-sentiment-db \
  -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=brand_sentiment \
  -p 5432:5432 postgres:15
poetry run alembic upgrade head
```

Copy `.env.example` to `.env` and fill in the values (`DATABASE_URL` is the only one the migrations use, but all settings fields are required). Then:

```bash
poetry run alembic check        # confirm models and migrations are in sync
poetry run pytest -v            # run the test suite
poetry run python -m ingestion.yelp_api.scraper   # live Yelp scrape to S3 (needs real keys)
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
| Reddit, App Store, News API scrapers | ⏳ Deferred until the pipeline works end to end |
| Postgres schema (`Review` model, async Alembic, model tests) | ✅ Complete |
| PySpark Glue ETL (raw JSONL → `reviews`) | 🔧 In progress |
| Airflow DAGs (`daily_ingest_dag`, `seed_data_dag`) | ⏳ Pending |
| Docker (per-service Dockerfiles, root `docker-compose.yml`, Makefile) | ⏳ Pending |
