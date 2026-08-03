# BlackRock EDP — POC Overview (One-Pager)

**Project:** Enterprise Data Platform — Snowflake-Native POC  
**Prepared by:** Snowflake Professional Services  
**Date:** July 29, 2026  

---

## Objective

Demonstrate an end-to-end, fully Snowflake-native data pipeline that ingests raw financial transaction data, validates it against automated quality rules, transforms it into analytics-ready models, and serves it for reporting — with zero external infrastructure.

---

## Pipeline at a Glance

```
Source Files (CSV)
       │
       ▼
┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│   MIRROR LAYER   │───►│    TDQ GATE      │───►│  STAGING LAYER   │───►│ PRESENTATION     │
│                  │    │                  │    │                  │    │                  │
│ Raw data loaded  │    │ Great Expect.    │    │ dbt transforms   │    │ Dynamic Tables   │
│ via COPY INTO    │    │ validates data   │    │ types + cleanses │    │ auto-refreshed   │
│ (all as strings) │    │ PASS → continue  │    │ incrementally    │    │ analytics marts  │
│                  │    │ FAIL → halt+alert│    │                  │    │                  │
└──────────────────┘    └──────────────────┘    └──────────────────┘    └──────────────────┘
```

---

## What Each Layer Does

| Layer | Purpose | Snowflake Feature |
|---|---|---|
| **Mirror** | Store raw source data exactly as received — no transformations | Internal Stage + COPY INTO |
| **TDQ Gate** | Validate completeness, uniqueness, format, and freshness before downstream processing | Great Expectations via Snowpark Python |
| **Staging** | Type-cast, cleanse, deduplicate, and apply dbt tests | dbt deployed natively via `snow dbt` |
| **Presentation** | Serve analytics-ready marts that auto-refresh within 15 minutes of upstream changes | Dynamic Tables + Cortex Analyst Semantic View |

---

## Data Quality Checks (TDQ Gate)

The following automated checks run after every load. If any fail, the pipeline halts and an alert fires:

- **Completeness** — Required fields (txn_id, account_id, amount, txn_date) must not be null
- **Uniqueness** — Transaction IDs must be unique (no duplicate records)
- **Schema conformance** — Column structure must match the agreed contract (detects drift)
- **Format validation** — Dates, IDs, and amounts must match expected patterns
- **Domain checks** — Currency and status values must be from an approved set
- **Freshness** — Data must have arrived within the expected SLA window

---

## Orchestration

The pipeline is scheduled every 4 hours and fully automated using Snowflake Tasks:

**ingest** → **validate** → **transform** → **log success**

If validation fails, the pipeline stops automatically — no bad data reaches the analytics layer.

---

## POC Scope

| Item | POC Assumption |
|---|---|
| Source entity | Financial transactions (single entity) |
| File format | CSV |
| Schedule | Every 4 hours |
| Analytics freshness | 15 minutes (Dynamic Table lag) |
| Alerting | Email via Snowflake-native alerts |

All parameters are configurable for production. Additional entities (positions, trades, reference data) can be onboarded using the same pattern.

---

## Key Benefits

- **100% Snowflake-native** — No external schedulers, no external compute, no third-party orchestration
- **Data never leaves Snowflake** — Quality checks and transformations all run inside Snowflake compute
- **Fail-safe by design** — Bad data is blocked at the Mirror layer before it can affect analytics
- **Self-service ready** — Cortex Analyst enables natural language queries over the presentation layer
- **Production-extensible** — POC pattern scales to additional entities and tighter SLAs without architecture changes

---

## Next Steps

1. Review this design and confirm POC scope
2. Snowflake PS builds and demonstrates the end-to-end pipeline
3. Walk through results together and discuss production considerations
