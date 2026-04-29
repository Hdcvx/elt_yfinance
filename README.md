# ELT Pipeline — B3 Financial Data

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-3.2.1-017CEE?logo=apacheairflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-1.11-FF694B?logo=dbt&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-1.4+-FF4B4B?logo=streamlit&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green)

End-to-end ELT pipeline that collects daily stock prices from the Brazilian market (B3) via **yFinance**, transforms the data through a medallion architecture using **dbt** on top of **PostgreSQL**, orchestrates everything with **Apache Airflow**, and exposes a financial analysis dashboard built with **Streamlit + Plotly**.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Why these tools?](#why-these-tools)
- [Data Model](#data-model)
- [Dashboard](#dashboard)
- [Getting Started](#getting-started)
- [Services](#services)
- [Project Structure](#project-structure)
- [DAG: pipeline\_financeiro](#dag-pipeline_financeiro)
- [dbt Models](#dbt-models)
- [IA Layer](#ia-layer)
- [Useful Commands](#useful-commands)
- [License](#license)

---

## Overview

This project implements a production-ready ELT pipeline for B3 equity data, covering every stage of the data engineering lifecycle:

| Stage | Description |
|---|---|
| **Extract** | Daily OHLCV prices pulled from Yahoo Finance for 16 B3 stocks |
| **Load** | Raw data landed directly into PostgreSQL (`bronze.precos`) |
| **Transform** | dbt models clean, key, and enrich the data across three layers |
| **Orchestrate** | Airflow DAG runs Mon–Fri at 18:00, after market close |
| **Serve** | Streamlit dashboard with interactive candlestick charts, moving averages, and volume |
| **Analyze** | Optional Claude API integration for natural language queries over the gold layer |

---

## Architecture

```
                     ┌──────────────┐
                     │  yFinance    │  OHLCV data — 16 B3 tickers
                     │  (Yahoo API) │
                     └──────┬───────┘
                            │  bronze.py
                            ▼
   ┌─────────────────────────────────────────────────────┐
   │                    PostgreSQL 16                    │
   │                                                     │
   │  bronze.precos                 ← raw table (upsert) │
   │       │                                             │
   │  [dbt run]                                          │
   │       │                                             │
   │  bronze.stg_precos           ← view  (clean + cast) │
   │       │                                             │
   │  silver.fct_precos_diarios  ← table (keyed + dated) │
   │       │                                             │
   │  gold.metricas_ativos       ← table (returns + MAs) │
   └──────────────────────┬──────────────────────────────┘
                          │
             ┌────────────┴────────────┐
             ▼                         ▼
       ┌─────────────┐          ┌──────────────┐
       │  Streamlit  │          │  Claude API  │
       │  Dashboard  │          │  (ia_layer)  │
       │  :8501      │          │  (optional)  │
       └─────────────┘          └──────────────┘

Orchestration: Apache Airflow 3.2.1 (runs every step above)
```

---

## Why these tools?

### Apache Airflow 3.x
The industry-standard workflow orchestrator. Version 3 introduces the decoupled **Execution API** architecture (separate webserver, scheduler and dag-processor processes), which improves stability and scalability. Used here to schedule the daily pipeline and handle task dependencies declaratively.

### dbt (data build tool)
dbt brings **software engineering practices to SQL**: version control, modular models, lineage tracking, and deterministic materialization strategies. The `generate_schema_name` macro override ensures models land in `bronze`, `silver`, and `gold` schemas — not dbt's default prefixed names.

### PostgreSQL 16
A single, robust engine serves as both the raw landing zone and the analytical serving layer, eliminating the need for an intermediate database. The `ON CONFLICT DO UPDATE` upsert pattern in the extraction step makes reruns idempotent.

### Streamlit + Plotly
Rapid dashboard development without a frontend stack. Plotly's `go.Candlestick` with `rangebreaks=[dict(bounds=["sat","mon"])]` removes non-trading days from the x-axis, producing clean financial charts. `@st.cache_data(ttl=300)` avoids redundant DB queries on re-renders.

### yFinance
Free, no-auth access to historical OHLCV data for B3 and global equities. `auto_adjust=True` returns split- and dividend-adjusted prices, which are more meaningful for multi-year analysis.

---

## Data Model

The pipeline follows the **medallion architecture**:

### Bronze — `bronze.precos`
Raw table populated directly by `bronze.py`. Upsert key: `(ticker, date)`. No transformations, only type coercion at insert time.

| Column | Type | Description |
|--------|------|-------------|
| `date` | DATE | Trading date |
| `ticker` | VARCHAR | Yahoo Finance ticker symbol |
| `open` | DOUBLE PRECISION | Adjusted opening price |
| `close` | DOUBLE PRECISION | Adjusted closing price |
| `high` | DOUBLE PRECISION | Adjusted daily high |
| `low` | DOUBLE PRECISION | Adjusted daily low |
| `volume` | BIGINT | Daily volume |

### Silver — `silver.fct_precos_diarios`
dbt **table** materialization. Adds a surrogate key, enforces data quality (filters incomplete OHLC rows), and stamps processing timestamps. Serves as the stable fact table for downstream models.

### Gold — `gold.metricas_ativos`
dbt **table** materialization. Computes analytical metrics using SQL window functions partitioned by ticker:

| Metric | Formula |
|--------|---------|
| `retorno_pct` | `(close - LAG(close)) / LAG(close) * 100` |
| `media_movel_7d` | `AVG(close) OVER (... ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)` |
| `media_movel_30d` | `AVG(close) OVER (... ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)` |

---

## Dashboard

The Streamlit app connects directly to `gold.metricas_ativos` and renders:

- **Candlestick chart** — OHLC with green/red candles, weekend gaps removed
- **Moving averages** — MM 7d (yellow) and MM 30d (purple) overlaid on the price chart
- **Volume** — color-coded bar chart (green = up day, red = down day)
- **Metrics bar** — last close, period return, high, low, and average volume

Available at `http://localhost:8501` after startup.

---

## Getting Started

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) and Docker Compose v2
- Git

### Quick Start

```bash
# 1. Clone the repository
git clone <repo-url>
cd elt_yfinance

# 2. Build images and start all services
docker compose up -d --build

# 3. Wait ~30 seconds, then open Airflow
open http://localhost:8080
# username: admin | password: admin
```

### Load Historical Data (first run)

On the first run, the database is empty. Load ~1 year of history for all tickers before triggering the DAG:

```bash
# Start only the database (if full stack is not yet up)
docker compose up postgres-dados -d

# Run the extraction locally — loads 365 days of OHLCV data
POSTGRES_HOST=localhost .venv/bin/python src/bronze.py

# Run dbt to populate silver and gold layers
DBT_POSTGRES_HOST=localhost .venv/bin/dbt run \
  --project-dir dbt --profiles-dir dbt
```

After this one-time load, the Airflow DAG takes over and runs incremental updates daily.

### Monitored Tickers

| Sector | Tickers |
|--------|---------|
| Oil & Energy | PETR4.SA · PRIO3.SA |
| Banks | ITUB4.SA · BBAS3.SA · BBDC4.SA · SANB11.SA |
| Commodities | VALE3.SA · GGBR4.SA · SUZB3.SA |
| Consumer & Retail | ABEV3.SA · LREN3.SA · MGLU3.SA |
| Industrial | WEGE3.SA · RENT3.SA |
| Healthcare | HAPV3.SA · RADL3.SA |

---

## Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Streamlit Dashboard | `http://localhost:8501` | — |
| Airflow UI | `http://localhost:8080` | `admin` / `admin` |
| pgAdmin | `http://localhost:5050` | `admin@admin.com` / `admin` |
| PostgreSQL | `localhost:5432` | `dados_user` / `dados_pass` |

### pgAdmin connection settings

| Field | Value |
|-------|-------|
| Host | `postgres-dados` (inside Docker) · `localhost` (external) |
| Port | `5432` |
| Database | `data_warehouse` |
| Username | `dados_user` |
| Password | `dados_pass` |

---

## Project Structure

```
elt_yfinance_v2/
│
├── dags/
│   └── pipeline_financeiro.py      # Airflow DAG — extract → transform
│
├── dbt/
│   ├── models/
│   │   ├── bronze/
│   │   │   └── stg_precos.sql      # view: clean + cast raw prices
│   │   ├── silver/
│   │   │   └── fct_precos_diarios.sql  # table: surrogate key + timestamps
│   │   └── gold/
│   │       └── metricas_ativos.sql # table: returns + moving averages
│   ├── macros/
│   │   └── generate_schema_name.sql   # override: use exact schema names
│   ├── dbt_project.yml
│   └── profiles.yml                # targets postgres-dados
│
├── src/
│   ├── bronze.py                   # yFinance → bronze.precos (upsert)
│   ├── dashboard.py                # Streamlit + Plotly dashboard
│   └── ia_layer.py                 # Claude API natural language queries
│
├── scripts/
│   └── init-db.sql                 # creates bronze / silver / gold schemas
│
├── .streamlit/
│   └── config.toml                 # dark theme configuration
│
├── Dockerfile                      # Airflow image (python deps)
├── Dockerfile.streamlit            # lightweight Streamlit image
├── docker-compose.yml              # full stack definition
└── pyproject.toml                  # project dependencies (managed by uv)
```

---

## DAG: `pipeline_financeiro`

**Schedule:** `0 18 * * 1-5` — weekdays at 18:00 (after B3 market close)

```
ingerir_bronze  ──▶  transformar_dbt
```

| Task | Action |
|------|--------|
| `ingerir_bronze` | Downloads the last 30 days of OHLCV data via yFinance and upserts into `bronze.precos` |
| `transformar_dbt` | Runs `dbt run` — refreshes `stg_precos` (view), `fct_precos_diarios` (table), and `metricas_ativos` (table) |

`catchup=False` ensures the DAG never backfills missed runs. The upsert pattern in `bronze.py` guarantees idempotency — re-running any day produces the same result.

---

## dbt Models

### `bronze.stg_precos` — view
Reads from `bronze.precos` and applies:
- Column renaming (English → Portuguese domain names)
- Type casting (`open::numeric` for `ROUND` compatibility with PostgreSQL)
- Data quality filter: excludes any row where `open`, `close`, `high`, or `low` is null or zero (guards against partial intraday data from yFinance)

### `silver.fct_precos_diarios` — table
Adds a deterministic surrogate key (`ticker || '_' || date`) and a `processado_em` timestamp. Acts as the stable fact table; no business logic lives here.

### `gold.metricas_ativos` — table
Applies window functions over the silver layer to compute:
- **Daily return** (`retorno_pct`): percentage change vs. prior close
- **7-day moving average** (`media_movel_7d`): short-term trend
- **30-day moving average** (`media_movel_30d`): long-term trend

Rows where `retorno_pct IS NULL` are excluded (first trading day per ticker, no prior close available).

---

## IA Layer

`src/ia_layer.py` provides a natural language interface over the gold layer using the **Anthropic Claude API**. It fetches the last N days of metrics, formats them as plain text, and passes them as context to the model.

```python
from src.ia_layer import perguntar

print(perguntar("Which asset had the highest return in the last 30 days?"))
print(perguntar("Is VALE3.SA trading above or below its 30-day moving average?"))
```

**Setup:**

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
.venv/bin/python src/ia_layer.py
```

> This module is standalone and independent from the Airflow pipeline.

---

## Useful Commands

```bash
# ── Docker ────────────────────────────────────────────────────────────────

# Start full stack (builds images on first run)
docker compose up -d --build

# Start only the database (lightweight local development)
docker compose up postgres-dados -d

# Stream logs from a specific container
docker logs elt_yfinance_v2-airflow-scheduler-1 -f

# Tear down everything including volumes (full reset)
docker compose down -v

# ── Airflow ───────────────────────────────────────────────────────────────

# Trigger the DAG manually
docker exec elt_yfinance_v2-airflow-scheduler-1 \
  airflow dags trigger pipeline_financeiro

# ── dbt ───────────────────────────────────────────────────────────────────

# Run all models locally
DBT_POSTGRES_HOST=localhost .venv/bin/dbt run \
  --project-dir dbt --profiles-dir dbt

# Run a single model
DBT_POSTGRES_HOST=localhost .venv/bin/dbt run \
  --project-dir dbt --profiles-dir dbt \
  --select gold.metricas_ativos

# ── PostgreSQL ────────────────────────────────────────────────────────────

# Open a psql session
docker exec -it elt_yfinance_v2-postgres-dados-1 \
  psql -U dados_user -d data_warehouse

# Check data coverage
docker exec elt_yfinance_v2-postgres-dados-1 \
  psql -U dados_user -d data_warehouse -c \
  "SELECT ticker, COUNT(*), MIN(data), MAX(data) FROM gold.metricas_ativos GROUP BY ticker ORDER BY ticker;"

# ── Streamlit ─────────────────────────────────────────────────────────────

# Run dashboard locally (requires postgres-dados running)
POSTGRES_HOST=localhost .venv/bin/streamlit run src/dashboard.py
```

---

## License

MIT © 2024
