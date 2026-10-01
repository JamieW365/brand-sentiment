# Brand Sentiment Analysis Platform — Architecture & Design

> **Status:** In development — P0 (shared package) complete, P1 (ingestion) in progress.
> Last updated: October 2026

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Repository Structure](#2-repository-structure)
3. [Technology Stack](#3-technology-stack)
4. [Architecture Decisions](#4-architecture-decisions)
5. [Shared Package](#5-shared-package)
6. [Data Pipeline](#6-data-pipeline)
7. [NLP Pipeline](#7-nlp-pipeline)
8. [API Layer](#8-api-layer)
9. [Frontend](#9-frontend)
10. [Infrastructure](#10-infrastructure)
11. [Development Conventions](#11-development-conventions)
12. [Build Phases & Timeline](#12-build-phases--timeline)
13. [Post-Launch: LangGraph Agent Extension](#13-post-launch-langgraph-agent-extension)

---

## 1. Project Overview

The Brand Sentiment Analysis Platform is an end-to-end NLP system for multi-dimensional brand sentiment analysis across multiple data sources. It ingests review and social data, scores it across three dimensions — sentiment, emotion, and aspect — and exposes results via a queryable API and interactive dashboard.

**Core dimensions scored per review:**

| Dimension | Detail |
|---|---|
| Sentiment | Positive / Neutral / Negative |
| Emotion | Anger / Joy / Sadness / Fear / Surprise |
| Aspect | Price / Service / Quality (zero-shot) |

**Data sources:**

| Source | Method |
|---|---|
| Yelp | Open Dataset + Fusion API |
| Amazon Reviews | HuggingFace / McAuley Lab (2023) |
| Reddit | PRAW (non-commercial free tier) |
| App Store | app-store-scraper |
| Google Play | google-play-scraper |
| News | NewsAPI.org |

> **Note:** Trustpilot and Google Reviews are explicitly excluded due to Terms of Service and scraping constraints.

---

## 2. Repository Structure

```
brand-sentiment/
├── ingestion/              # Data scrapers — one per source
│   ├── base.py             # Abstract BaseScraper class
│   ├── s3_writer.py        # S3 upload helper (wraps shared/aws/s3.py)
│   ├── yelp_api/
│   ├── reddit/
│   ├── app_store/
│   └── news_api/
├── etl/                    # PySpark Glue transformation jobs
│   ├── jobs/
│   ├── schemas/
│   └── tests/
├── orchestration/          # Apache Airflow DAGs and operators
│   ├── dags/
│   │   ├── daily_ingest_dag.py
│   │   ├── nlp_scoring_dag.py
│   │   └── seed_data_dag.py
│   ├── plugins/
│   └── operators/
├── nlp/                    # HuggingFace NLP pipeline + MLFlow
│   ├── models/
│   ├── pipelines/
│   ├── mlflow/
│   └── training/
├── api/                    # FastAPI application
│   ├── routers/
│   ├── models/
│   ├── db/
│   ├── cache/
│   │   └── redis.py
│   └── main.py
├── frontend/               # Streamlit dashboard
│   ├── pages/
│   ├── components/
│   └── utils/
│       └── api_client.py
├── infra/                  # Terraform infrastructure definitions
│   └── terraform/
├── data/
│   ├── raw/                # Never committed — S3 only
│   ├── processed/          # Never committed — S3 only
│   └── migrations/         # Alembic DB migration scripts
├── shared/                 # Internal package — imported by all services
│   ├── config/
│   │   └── settings.py     # Pydantic BaseSettings
│   ├── logging/
│   │   └── logger.py       # structlog JSON logging
│   ├── db/
│   │   ├── base.py         # SQLAlchemy DeclarativeBase
│   │   └── session.py      # Async engine + session factory
│   └── aws/
│       └── s3.py           # boto3 S3 read/write helper
├── .github/
│   └── workflows/          # GitHub Actions CI/CD pipelines
├── docker-compose.yml      # Local development environment
├── pyproject.toml          # Poetry — single source of truth for dependencies
├── Makefile                # Developer UX (make up / make ingest / make score)
├── .env.example            # Environment variable template
└── README.md
```

---

## 3. Technology Stack

| Layer | Technology | Rationale |
|---|---|---|
| Language | Python 3.12 | Latest stable; strong ML ecosystem; 3.10 EOL Oct 2026 |
| Dependency management | Poetry 2.5.1 | Monorepo workspaces; lockfile reproducibility; dependency groups |
| Configuration | Pydantic Settings | Type-safe env validation; single config object across services |
| Logging | structlog | Structured JSON output; queryable in CloudWatch; production standard |
| ORM | SQLAlchemy 2.x (async) | Non-blocking DB calls inside FastAPI async event loop |
| Database driver | asyncpg | Native async Postgres driver required by SQLAlchemy asyncio |
| AWS SDK | boto3 | Official AWS Python SDK |
| NLP | HuggingFace Transformers | State-of-the-art pretrained models; no fine-tuning required |
| Model registry | MLFlow | Experiment tracking; score distribution logging per run |
| Data processing | PySpark (AWS Glue) | Distributed ETL at scale; managed serverless execution |
| Orchestration | Apache Airflow (MWAA) | DAG-based pipeline scheduling; managed AWS service |
| API | FastAPI | Async-native; automatic OpenAPI docs; high performance |
| Caching | Redis | Low-latency API response caching |
| Frontend | Streamlit | Rapid ML dashboard development; Python-native |
| Containers | Docker + Docker Compose | Reproducible local dev; production deployment via ECS |
| Infrastructure | Terraform | Infrastructure as code; reproducible AWS provisioning |
| CI/CD | GitHub Actions | Native GitHub integration; ECR/ECS deploy pipeline |
| Container registry | AWS ECR | Native ECS integration |
| Compute | AWS ECS Fargate | Serverless containers; no EC2 management |
| Object storage | AWS S3 | Raw and processed data lake |
| Relational DB | AWS RDS Postgres | Scored review storage; pgvector extension for agent |
| Secrets | AWS Secrets Manager | Production credential management |

---

## 4. Architecture Decisions

### 4.1 Monorepo over polyrepo

**Decision:** Single repository for all services.

**Rationale:** Shared code (config, logging, DB, S3) is importable without publishing to PyPI. Atomic commits across service boundaries. Simpler CI/CD for a single-developer project. Easier for portfolio reviewers to understand the full system in one place.

**Trade-off:** As the project scales, a monorepo requires discipline to keep service boundaries clean.

---

### 4.2 `shared/` as an internal Poetry package

**Decision:** `shared/` is registered in `pyproject.toml` as an installable internal package rather than using `sys.path` manipulation.

**Rationale:** Clean imports across all services (`from shared.config.settings import get_settings`). No fragile path hacks. Consistent with how production Python monorepos are managed.

---

### 4.3 Lazy instantiation via `@lru_cache` factories

**Decision:** All expensive objects (Settings, SQLAlchemy engine, boto3 client) are created inside `@lru_cache` factory functions rather than at module import time.

**Rationale:** Module-level instantiation causes failures in environments without a full `.env` (unit tests, Spark jobs, CI runners). The factory pattern defers instantiation until the object is explicitly requested, and caches the result so it is only created once.

```python
# Pattern used across shared/
@lru_cache
def get_settings() -> Settings:
    return Settings()
```

---

### 4.4 Poetry dependency groups

**Decision:** Dependencies are split into named groups — `default`, `db`, `aws`, `dev` — rather than a flat install.

**Rationale:** A Spark/Glue job importing `shared.logging` should not pull in `asyncpg` or `sqlalchemy`. Keeping groups separate reduces Docker image sizes and avoids unnecessary dependency resolution in environments that don't need the full stack.

| Group | Contents | Installed by |
|---|---|---|
| default | pydantic-settings, structlog | all services |
| db | sqlalchemy[asyncio], asyncpg | API, migrations |
| aws | boto3 | ingestion, ETL |
| dev | pytest, pytest-asyncio | local development only |

---

### 4.5 Async SQLAlchemy

**Decision:** SQLAlchemy is configured for async operation throughout.

**Rationale:** FastAPI is an async framework. Synchronous database calls inside an async application block the event loop — while one request waits for a DB query, no other requests can be handled. Async SQLAlchemy with asyncpg makes all database operations non-blocking.

---

### 4.6 Abstract base class for scrapers

**Decision:** All ingestion scrapers inherit from a `BaseScraper` abstract class.

**Rationale:** Enforces a consistent interface across all data sources. New scrapers are guaranteed to implement the required methods. Simplifies orchestration — the Airflow DAG can treat all scrapers uniformly.

---

### 4.7 S3 as the data lake

**Decision:** All raw ingested data lands in S3 before any processing. ETL reads from S3, not directly from scrapers.

**Rationale:** Decouples ingestion from transformation. Raw data is preserved and reprocessable. Follows the standard medallion architecture (raw → processed → scored).

---

## 5. Shared Package

The `shared/` package is the foundation imported by every service. It must remain lightweight and free of service-specific dependencies.

### 5.1 `shared/config/settings.py`

Pydantic `BaseSettings` subclass. Reads all configuration from environment variables and `.env` file. Accessed via `get_settings()` factory.

```python
from shared.config.settings import get_settings
settings = get_settings()
print(settings.aws_region)  # "eu-west-2"
```

**Required environment variables:**

| Variable | Used by |
|---|---|
| `DATABASE_URL` | SQLAlchemy engine |
| `REDIS_URL` | API cache |
| `AWS_ACCESS_KEY_ID` | boto3 |
| `AWS_SECRET_ACCESS_KEY` | boto3 |
| `AWS_REGION` | boto3 (default: eu-west-2) |
| `S3_BUCKET_NAME` | S3Writer |
| `MLFLOW_TRACKING_URI` | NLP pipeline |
| `NEWSAPI_KEY` | News API scraper |
| `REDDIT_CLIENT_ID` | PRAW |
| `REDDIT_CLIENT_SECRET` | PRAW |
| `REDDIT_USER_AGENT` | PRAW |
| `YELP_API_KEY` | Yelp Fusion API |

---

### 5.2 `shared/logging/logger.py`

structlog configured to output structured JSON. Every log line is a queryable JSON object in CloudWatch.

```python
from shared.logging.logger import setup_logging, get_logger

setup_logging()  # call once at service startup
logger = get_logger(__name__)
logger.info("ingestion started", brand="McDonald's", source="yelp")
```

**Output:**
```json
{
  "event": "ingestion started",
  "level": "info",
  "logger": "ingestion.yelp_api",
  "timestamp": "2026-10-01T09:00:00Z",
  "brand": "McDonald's",
  "source": "yelp"
}
```

---

### 5.3 `shared/db/base.py` + `session.py`

SQLAlchemy async engine and session factory. All ORM models inherit from `Base`. Database sessions are managed via the `get_db()` FastAPI dependency.

```python
from shared.db.base import Base
from shared.db.session import get_db, get_engine
```

---

### 5.4 `shared/aws/s3.py`

boto3 S3 helper. Single cached client shared across all `S3Writer` instances.

```python
from shared.aws.s3 import S3Writer

writer = S3Writer()
writer.write_json(data=review_dict, s3_key="raw/yelp/2026-10-01/batch_001.json")
```

---

## 6. Data Pipeline

```
[Scrapers] → [S3 raw/] → [PySpark Glue ETL] → [S3 processed/] → [Postgres]
                                                                        ↓
                                                              [NLP Scoring Pipeline]
                                                                        ↓
                                                              [Postgres scored_reviews]
```

### 6.1 Ingestion (P1)

- Abstract `BaseScraper` in `ingestion/base.py`
- One scraper per source inheriting `BaseScraper`
- All scrapers write raw JSON to S3 via `S3Writer`
- S3 key structure: `raw/{source}/{date}/{batch_id}.json`
- Orchestrated by `daily_ingest_dag` in Airflow

### 6.2 ETL (P1)

- PySpark jobs running on AWS Glue
- Reads raw JSON from S3
- Applies schema validation and normalisation
- Writes processed Parquet to S3 and structured records to Postgres
- S3 key structure: `processed/{source}/{date}/`

### 6.3 NLP Scoring (P2)

- HuggingFace pipeline runs against processed records
- Three models run per review (sentiment, emotion, aspect)
- Scores written back to Postgres `scored_reviews` table
- MLFlow logs score distributions per run
- Orchestrated by `nlp_scoring_dag` in Airflow

---

## 7. NLP Pipeline

### 7.1 Models

| Task | Model | Output |
|---|---|---|
| Sentiment | `cardiffnlp/twitter-roberta-base-sentiment-latest` | positive / neutral / negative + confidence |
| Emotion | `j-hartmann/emotion-english-distilroberta-base` | anger / joy / sadness / fear / surprise + confidence |
| Aspect (zero-shot) | `facebook/bart-large-mnli` | price / service / quality relevance scores |

### 7.2 MLFlow

MLFlow is used for:
- Model registry — versioned model storage
- Run logging — score distributions per inference run
- **Not** used for fine-tuning (models are used as-is)

### 7.3 Zero-shot aspect classification

`facebook/bart-large-mnli` performs Natural Language Inference (NLI) between the review text and candidate aspect labels. No labelled training data required.

```python
# Conceptual example
classifier("The queue was too long", candidate_labels=["price", "service", "quality"])
# → {"service": 0.82, "quality": 0.11, "price": 0.07}
```

---

## 8. API Layer

FastAPI application serving scored sentiment data.

- Async throughout — non-blocking DB queries via SQLAlchemy asyncio
- Redis cache on high-traffic endpoints (brand summary, top reviews)
- Automatic OpenAPI documentation at `/docs`
- `get_db()` dependency injection for database sessions

**Planned endpoints:**

| Endpoint | Description |
|---|---|
| `GET /brands/{brand_id}/sentiment` | Aggregate sentiment scores |
| `GET /brands/{brand_id}/emotions` | Emotion distribution |
| `GET /brands/{brand_id}/aspects` | Aspect scores breakdown |
| `GET /reviews/search` | Full-text review search |
| `GET /brands/compare` | Side-by-side brand comparison |

---

## 9. Frontend

Streamlit dashboard with four core views:

| Page | Description |
|---|---|
| Search | Review search and filtering |
| Sentiment Dashboard | Sentiment trends over time |
| Emotion Radar | Emotion distribution radar chart |
| Aspect Scores | Price / service / quality breakdown |
| Brand Comparison | Side-by-side multi-brand view |

---

## 10. Infrastructure

All AWS infrastructure defined in Terraform under `infra/terraform/`.

### 10.1 AWS Services

| Service | Purpose |
|---|---|
| S3 | Raw and processed data lake |
| RDS Postgres | Scored review storage + pgvector |
| AWS Glue | Managed PySpark ETL |
| MWAA | Managed Airflow orchestration |
| ECS Fargate | Serverless container compute |
| ECR | Docker image registry |
| Secrets Manager | Production credential storage |

### 10.2 Local development

Docker Compose runs the full stack locally:

```bash
make up      # start all services
make ingest  # run ingestion scrapers
make score   # run NLP scoring pipeline
```

---

## 11. Development Conventions

### 11.1 Branching — GitHub Flow

```
main  ←  always deployable
  └── feature/ingestion-base-scraper
  └── fix/redis-connection-timeout
  └── chore/terraform-ecs-setup
```

- Every piece of work gets its own branch cut from `main`
- Branch naming: `type/short-description-in-kebab-case`
- Merge via PR — review your own diff before merging
- Delete branch after merge

### 11.2 Commits — Conventional Commits

```
<type>(<scope>): <description>
```

| Type | When |
|---|---|
| `feat` | New functional code |
| `fix` | Bug fix or correction |
| `chore` | Tooling, setup, housekeeping |
| `docs` | Documentation only |
| `test` | Tests only |
| `refactor` | Code restructure, no behaviour change |
| `ci` | CI/CD pipeline changes |
| `perf` | Performance improvements |
| `build` | Dependency or build system changes |

**Examples used in this project:**
```
feat(shared): add Pydantic settings configuration module
fix(shared): replace deprecated format_exc_info with ExceptionRenderer
chore: restructure dependencies into Poetry groups (db, aws, dev)
fix: move .github/workflows to repo root for GitHub Actions to pick up
```

### 11.3 Environment

- Ubuntu 22.04.5 LTS
- Python 3.12.14 (deadsnakes PPA — system Python 3.10 left untouched)
- Poetry 2.5.1 (`virtualenvs.in-project = true`)
- All project code runs through Poetry-managed `.venv`

---

## 12. Build Phases & Timeline

| Phase | Scope | Duration |
|---|---|---|
| **P0 — Foundation** | Repo setup, Poetry, shared package | ✅ Complete |
| **P1 — Ingestion** | Scrapers, S3, PySpark Glue ETL, Postgres, Airflow DAG, Docker | 1.5 – 2 wks |
| **P2 — NLP** | HuggingFace pipeline, MLFlow registry, Airflow scoring task | 1.5 – 2 wks |
| **P3 — API** | FastAPI, Redis cache | 1 wk |
| **P4 — Frontend** | Streamlit dashboard | 1 wk |
| **P5 — Infrastructure** | GitHub Actions CI/CD, ECR/ECS deploy, Terraform, Secrets Manager | 1 wk |

**Total: ~7 weeks**

---

## 13. Post-Launch: LangGraph Agent Extension

A ReAct agent extension scoped for after P5. Architecture already designed to slot into the existing monorepo.

### 13.1 Overview

A LangGraph ReAct loop with conditional routing between two tools:

| Tool | Purpose |
|---|---|
| `query_postgres` | Structured SQL queries against scored_reviews |
| `search_reviews` | Semantic similarity search via pgvector |

### 13.2 Integration points

- Agent router added to `api/routers/`
- pgvector extension added to RDS Postgres
- Vector embeddings migration in `data/migrations/`
- Observability via Weights & Biases Weave

### 13.3 Key interview talking points

- ReAct loop mechanics — reason, act, observe cycle
- Hallucination prevention via grounded tool outputs
- Vector similarity search with pgvector
- Production observability patterns with W&B Weave
- Conditional routing between structured and semantic retrieval

---

*This document is maintained alongside the codebase. Update it when architectural decisions change.*
