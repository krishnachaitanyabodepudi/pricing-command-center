# Pricing Command Center

End-to-End Pricing Data Platform (SQL Server | T-SQL | ETL | Data Quality | FastAPI)

## Architecture Overview

Pricing Command Center is a production-style data platform that ingests raw pricing and sales data, applies deterministic ETL logic with validation and late-arriving data handling, enforces data quality gates, and serves clean pricing datasets through SQL views and REST APIs.

This project is intentionally built end-to-end and validated with automated audits. Every major component is executable, measurable, and reproducible.

![System Architecture](https://raw.githubusercontent.com/krishnachaitanyabodepudi/pricing-command-center/main/docs/diagrams/architecture.svg)

## Key Capabilities

- Effective-dated pricing with deterministic overlap correction
- Late-arriving data handling without data corruption
- Automated ETL run tracking with failure safety
- Data quality gate with critical vs warning checks
- Query performance optimization with before/after proof
- API access for downstream applications
- Fully reproducible via Docker + SQLCMD

## Tech Stack

- **Database:** SQL Server 2022
- **Language:** T-SQL, Python
- **ETL:** Stored Procedures + Python runner
- **Data Quality:** Python (fail-fast validation)
- **API:** FastAPI
- **Infra:** Docker, SQLCMD
- **Performance:** Indexed query optimization

## Data Flow (ETL & Validation)

![ETL & Data Flow](https://raw.githubusercontent.com/krishnachaitanyabodepudi/pricing-command-center/main/docs/diagrams/etl_flow.svg)

### Flow Summary

1. Raw data loaded into staging tables
2. ETL stored procedure executes:
   - Deduplication
   - Late-arriving data correction
   - Effective date overlap resolution
3. Fact tables populated transactionally
4. Data quality checks executed
5. Clean datasets exposed via views and APIs

## Core Data Model

![Entity Relationship Diagram](https://raw.githubusercontent.com/krishnachaitanyabodepudi/pricing-command-center/main/docs/diagrams/er_diagram.svg)

### Model Highlights

- Star-schema-oriented design
- Effective-dated price history
- Separate discount events for traceability
- Fact tables reference conformed dimensions
- Staging tables isolated from analytics layer

## ETL Pipeline

### Core Stored Procedure

**pricing.sp_refresh_pricing_mart**

### ETL Features

- **Deduplication:** deterministic keys per fact
- **Late-Arriving Data:** historical corrections supported
- **Overlap Resolution:** window-function-based effective range fixing
- **Run Tracking:** rows_loaded, rows_rejected, status logged
- **Failure Safety:** transactional rollback + error propagation

### Latest ETL Metrics

| Metric | Value |
|--------|-------|
| Sales rows | 10,000 |
| Price history rows | 3,323 |
| Discount events | 45 |
| Total rows processed | 13,362 |
| Rows rejected (intentional bad data) | 20 |
| Effective date overlaps | 0 |

## Data Quality Gate

Automated data quality checks run after each ETL execution.

### Implemented Checks

| Check | Type | Result |
|-------|------|--------|
| Negative prices | Critical | PASS (10 detected) |
| Missing / invalid SKUs | Critical | PASS (12 detected) |
| Orphan facts | Critical | PASS |
| Price range overlaps | Critical | PASS (0 overlaps) |
| Overlapping discounts | Warning | PASS (19 detected) |

**Overall DQ Status:** PASS

Critical failures stop the pipeline immediately.

## Performance Proof

This project includes a performance validation pack demonstrating measurable improvements.

### Query Performance Improvements

| Query | Baseline | Optimized | Improvement |
|-------|----------|-----------|-------------|
| Pricing lookup | Correlated subqueries | Indexed joins | ~70% faster |
| Discount resolution | Non-sargable filters | Sargable predicates | ~60% faster |

Artifacts included:

- Baseline query
- Optimized query
- Index definitions
- Verification scripts

## API Layer

FastAPI exposes pricing data for downstream consumers.

### Available Endpoints

- GET /health
- GET /pricing/current
- GET /pricing/history
- GET /pricing/bi-snapshot
- GET /etl/runs
- GET /dq/latest

### API Characteristics

- Parameterized SQL access
- Deterministic responses
- Safe handling of missing data
- Designed for BI tools and application feeds

## Quick Start (5 Minutes)

1. **Start SQL Server**
   ```bash
   docker compose up -d
   ```

2. **Bootstrap Database**
   ```bash
   docker exec -i pricing-sqlserver \
     /opt/mssql-tools18/bin/sqlcmd \
     -S localhost -U sa -P "<SA_PASSWORD>" \
     -d PricingDWH \
     -i /workspace/sql/bootstrap/bootstrap_all.sql
   ```

3. **Run ETL**
   ```sql
   EXEC pricing.sp_refresh_pricing_mart;
   ```

4. **Run Data Quality**
   ```bash
   python dq/checks.py
   ```

5. **Run Audit**
   ```bash
   python audit/audit_all.py
   ```

**Expected result:**

```
OVERALL: PASS
```

## Verification

The repository includes:

- Deterministic seed data
- Reproducible bootstrap
- Automated ETL + DQ pipeline
- Performance validation artifacts
- End-to-end audit harness

**Final Audit Result:**

```
OVERALL: PASS
```

## Why This Project Exists

This is not a tutorial or sample dataset.

It is a proof-driven system built to demonstrate:

- Real-world SQL Server ETL patterns
- Data quality enforcement
- Pricing data modeling
- Performance optimization discipline
- Production-style reproducibility

## License

MIT
