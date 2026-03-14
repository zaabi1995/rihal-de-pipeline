# Shipment Analytics Pipeline

A production-hardened Airflow ETL pipeline that processes shipment data from a REST API and customer tiers from CSV, producing monthly shipping spend analytics by customer tier.

## Architecture

```
                    ┌──────────────────────────────────┐
                    │        Docker Compose             │
                    │                                   │
 ┌─────────┐       │  ┌─────────┐    ┌──────────────┐ │
 │ Mock API │◄──────┼──│ Airflow │    │  PostgreSQL   │ │
 │ :8000    │       │  │ :8080   │───►│  :5432        │ │
 └─────────┘       │  └────┬────┘    │               │ │
                    │       │         │  staging.*    │ │
 ┌─────────┐       │  ┌────▼────┐    │  analytics.*  │ │
 │ CSV File │◄──────┼──│Scheduler│───►│               │ │
 └─────────┘       │  └─────────┘    └──────────────┘ │
                    └──────────────────────────────────┘

Pipeline Flow:
┌──────────────┐  ┌──────────────────┐  ┌───────────────┐  ┌──────────────┐
│   Extract    │  │     Extract      │  │   Transform   │  │     Load     │
│  Shipments   │  │  Customer Tiers  │  │  & Clean &    │  │  Analytics   │
│  (from API)  │  │  (from CSV)      │  │  Join         │  │  (aggregate) │
└──────┬───────┘  └────────┬─────────┘  └───────┬───────┘  └──────────────┘
       │                   │                     │                  ▲
       └───────────────────┴─────────►───────────┴──────────────────┘
              parallel extract              sequential transform + load
```

## What Was Fixed

This pipeline was inherited with several critical issues. Here's what was identified and fixed:

| Issue | Severity | Fix |
|-------|----------|-----|
| Port conflict (API & Airflow both on 8080) | HIGH | API moved to port 8000 |
| SQL injection in extract_shipments.py | HIGH | Parameterized queries (%s) |
| No API error handling or retry | HIGH | 3 retries with exponential backoff |
| No idempotency in analytics load | HIGH | TRUNCATE before INSERT |
| DROP+CREATE staging pattern | MEDIUM | CREATE IF NOT EXISTS + TRUNCATE |
| Duplicate shipment SHP002 | MEDIUM | Dedup by shipment_id (last-write-wins) |
| Bad data not filtered | MEDIUM | Filters for null, negative, zero, cancelled |
| SCD not handled for customer tiers | MEDIUM | LATERAL join with tier_updated_date |
| Hardcoded credentials | MEDIUM | Environment variables |
| No tests | MEDIUM | 28 tests across 3 test files |
| Weak DAG retry config | LOW | 3 retries, 2-minute delay |
| No database constraints | LOW | NOT NULL, CHECK, indexes |

Full details in [ENGINEERING_AUDIT.md](ENGINEERING_AUDIT.md).

## Quick Start

### Prerequisites
- Docker Desktop installed and running
- Docker Compose
- At least 4GB of available RAM

### Run the Pipeline

```bash
# Start all services
docker-compose up -d

# Wait ~2-3 minutes for initialization, then check:
docker-compose ps

# Access Airflow UI
open http://localhost:8080    # Username: admin, Password: admin

# Trigger the pipeline:
# 1. Find "shipment_analytics_pipeline" in the DAG list
# 2. Toggle it ON
# 3. Click the Play button for a manual run

# Check results
docker-compose exec postgres psql -U airflow -d airflow -c \
  "SELECT * FROM analytics.shipping_spend_by_tier ORDER BY year_month, tier;"
```

### Service URLs
- **Airflow UI:** http://localhost:8080 (admin/admin)
- **Mock API:** http://localhost:8000/api/shipments
- **PostgreSQL:** localhost:5432 (airflow/airflow)

### Run Tests

```bash
# Inside the Airflow container
docker-compose exec airflow-webserver pytest /opt/airflow/tests/ -v

# Or locally (requires Python 3.9+ with pytest, requests, psycopg2-binary, pandas)
python -m pytest tests/ -v
```

### Stop

```bash
docker-compose down        # Stop services
docker-compose down -v     # Stop and delete all data
```

## Project Structure

```
.
├── dags/
│   └── shipment_analytics_dag.py    # Airflow DAG definition
├── scripts/
│   ├── extract_shipments.py         # Extract from API (retry, parameterized SQL)
│   ├── extract_customer_tiers.py    # Extract from CSV (validation)
│   ├── transform_data.py            # Filter, dedup, SCD join
│   └── load_analytics.py            # Idempotent aggregation load
├── sql/
│   └── init.sql                     # Schema, tables, constraints, indexes
├── data/
│   └── customer_tiers.csv           # Customer tier source data
├── api/
│   ├── app.py                       # Mock shipment API (Flask)
│   └── Dockerfile
├── tests/
│   ├── conftest.py                  # Fixtures, mock DB, sample data
│   ├── test_extract_shipments.py    # Retry, SQL injection prevention
│   ├── test_transform.py            # Data quality filters, SCD, dedup
│   └── test_load.py                 # Idempotency verification
├── docker-compose.yml               # Service orchestration
├── Dockerfile                       # Airflow image with dependencies
├── ENGINEERING_AUDIT.md             # Detailed issue audit
├── DESIGN_REFLECTION.md             # Design decisions and trade-offs
└── README.md
```

## Data Quality Rules

The transform step applies these filters (in order):

1. **Dedup:** Multiple rows with the same `shipment_id` are reduced to one (most recently loaded wins)
2. **Null customer:** Shipments with no `customer_id` are dropped (can't attribute to a tier)
3. **Invalid cost:** Shipments with `shipping_cost <= 0` are dropped
4. **Cancelled:** Shipments with `status = 'cancelled'` are excluded
5. **SCD join:** Each shipment is matched to the customer tier effective on the shipment date
6. **Unknown tier:** Customers not in the tier table get `tier = 'Unknown'`

## Useful Commands

```bash
# View logs
docker-compose logs airflow-scheduler -f
docker-compose logs api -f
docker-compose logs postgres -f

# Connect to database
docker-compose exec postgres psql -U airflow -d airflow

# Check staging data
docker-compose exec postgres psql -U airflow -d airflow -c "SELECT * FROM staging.shipments;"
docker-compose exec postgres psql -U airflow -d airflow -c "SELECT * FROM staging.customer_tiers;"

# Check transformed data
docker-compose exec postgres psql -U airflow -d airflow -c "SELECT * FROM staging.shipments_with_tiers;"

# Check API
curl http://localhost:8000/api/shipments | python -m json.tool

# Restart Airflow
docker-compose restart airflow-scheduler airflow-webserver
```
