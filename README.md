# Agricultural Commodity Price Intelligence Pipeline

An end-to-end data pipeline that ingests daily commodity price data from Indian public agricultural markets, transforms it into an analytics-ready warehouse, and surfaces insights through an interactive dashboard — built to help smallholder farmers and traders make better selling and cropping decisions.

**Status:** In active development · Target v1: 3 weeks

---

## Why this project

India's agricultural markets generate millions of price observations every day across 3,000+ physical market centers (*mandis*), all published openly via the **Agmarknet** and **e-NAM** portals run by the Ministry of Agriculture. The data is technically free, but:

- It's fragmented across hundreds of commodity and market combinations
- Prices vary wildly day-to-day and region-to-region
- Seasonal arbitrage opportunities exist but aren't visible without stitching the data together
- Smallholder farmers (who make up the majority of Indian agriculture) rarely see consolidated price intelligence

I come from a farming family in Prakasam district, Andhra Pradesh — ~10 acres of mixed-soil land currently in transition away from tobacco. This project is a practical tool I'm building to inform cropping and market-access decisions for that farm, and a showcase of the data engineering and analytics stack at the same time.

## What this project does

- Ingests daily price records from public APIs on a scheduled cadence
- Loads raw data into a PostgreSQL warehouse with dbt-managed transformations
- Produces cleaned, analytics-ready fact and dimension tables covering 200+ commodities across 3,000+ market centers
- Serves a Power BI dashboard covering price trends, seasonality, regional arbitrage opportunities, and crop-level margin analysis
- Includes a time-series forecasting layer (ARIMA / Prophet) for 30-day price prediction on selected crops

## Tech stack

| Layer | Tool | Why |
|---|---|---|
| Ingestion | Python + `requests` | Lightweight, explicit, easy to reason about for scheduled pulls |
| Orchestration | GitHub Actions (cron) | Free, version-controlled, no infra to manage for v1 |
| Storage | PostgreSQL (Supabase free tier) | Solid warehouse semantics, 500MB is plenty for v1 |
| Transformation | dbt Core | Industry-standard, makes data modeling readable and testable |
| Visualization | Power BI | Business-facing, strong for the target audience |
| Forecasting | `statsmodels`, `prophet` | Honest baseline models with clear error reporting |

## Architecture

```
Agmarknet / e-NAM APIs
         │
         ▼
  Python ingestion (GitHub Actions cron, daily)
         │
         ▼
  PostgreSQL (raw schema)
         │
         ▼
  dbt Core transformations (staging → marts)
         │
         ├──► Power BI dashboards (trends, arbitrage, margins)
         └──► Forecasting notebooks (ARIMA, Prophet)
```

## Repository layout

```
.
├── ingestion/         # Python scripts for API pulls
├── dbt/               # dbt project (staging + marts models)
├── notebooks/         # EDA + forecasting notebooks
├── dashboards/        # Power BI .pbix files + screenshots
├── .github/workflows/ # scheduled ingestion workflow
└── README.md
```

## Sample questions this answers

- Which crops in Andhra Pradesh have shown the widest price dispersion across neighboring mandis this month?
- When in the harvest cycle does cotton typically hit its annual price peak?
- Is the current price of groundnut in Prakasam unusually low relative to its five-year seasonal pattern?
- For a grower with mixed soil, what crop portfolio optimizes expected margin net of price volatility?

## Getting started *(will be filled in as build progresses)*

```bash
# clone
git clone https://github.com/NithinKilari/<repo-name>.git
cd <repo-name>

# Python env
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# configure DB credentials in .env (see .env.example)
# run initial ingestion
python ingestion/run.py
```

## Roadmap

- [ ] v0.1 — ingestion script for one commodity / one state, loaded into Postgres
- [ ] v0.2 — dbt staging + mart models, daily GitHub Actions schedule
- [ ] v0.3 — Power BI dashboard v1 (trend + seasonality)
- [ ] v0.4 — regional arbitrage view
- [ ] v0.5 — ARIMA / Prophet forecasting with backtested error bounds
- [ ] v1.0 — published dashboard link + writeup on findings for Prakasam crops

## About the author

Nithin Kilari — M.S. in Computer Science (Data Science), Oklahoma City University. Interested in data engineering, analytics, and applying modern data tooling to the agricultural sector.

[LinkedIn](https://www.linkedin.com/in/kilari-nithin-619481272/) · [GitHub](https://github.com/NithinKilari)
