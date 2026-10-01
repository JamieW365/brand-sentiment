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

```bash
make up       # start all services
make ingest   # run ingestion scrapers
make score    # run NLP scoring pipeline
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
