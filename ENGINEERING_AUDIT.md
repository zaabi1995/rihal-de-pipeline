# Engineering Audit

**Auditor:** Ali Al Zaabi
**Date:** 2026-03-14
**Scope:** Full pipeline review — extraction, transformation, loading, orchestration, and infrastructure

---

## Executive Summary

After cloning the repository and tracing through each layer of the pipeline, I found 10 distinct issues ranging from a port conflict that prevents the stack from starting, to a SQL injection vulnerability that could allow data exfiltration. Several of these compound: duplicate records flow through unchecked transforms into analytics that double-count on every re-run. None of the issues are theoretical — each one manifests on a normal `docker-compose up` followed by a DAG trigger.

---

## Issues Discovered

### Issue 1: Port Conflict — Stack Fails to Start

- **Severity:** HIGH
- **File:** `docker-compose.yml`, lines 28 and 57
- **Description:** Both the `api` service and `airflow-webserver` map to host port `8080`. Docker will refuse to bind the second container, so either the API or Airflow never starts.
- **Impact:** The pipeline cannot run at all in its default configuration. A new contributor cloning the repo and running `docker-compose up` will immediately see a bind error and assume the project is broken.
- **Mitigation:** Changed the API host port to `8000` (`"8000:8000"`), keeping Airflow on `8080`. Updated README to reflect the correct ports.

---

### Issue 2: SQL Injection in Shipment Extraction

- **Severity:** HIGH
- **File:** `scripts/extract_shipments.py`, lines 45–49
- **Description:** Shipment fields are interpolated directly into SQL via f-strings:
  ```python
  cursor.execute(f"""
      INSERT INTO staging.shipments ...
      VALUES ('{shipment['shipment_id']}', '{shipment['customer_id']}', ...)
  """)
  ```
  Any shipment field containing a single quote (or a crafted payload) breaks the query or executes arbitrary SQL.
- **Impact:** If the upstream API is compromised or returns malicious data, an attacker could read, modify, or delete any data in the PostgreSQL instance — including Airflow's own metadata tables.
- **Mitigation:** Replaced f-string interpolation with parameterized queries using `%s` placeholders, matching the pattern already used in `extract_customer_tiers.py`.

---

### Issue 3: No API Error Handling or Retry Logic

- **Severity:** HIGH
- **File:** `scripts/extract_shipments.py`, lines 24–26
- **Description:** The API call uses a bare `requests.get()` with no timeout, no status code check, and no retry. The mock API itself simulates 500 errors (every 10th request) and 5-second delays (every 7th request). The code blindly calls `response.json()` — a 500 response will either fail to parse or return an error object without a `data` key.
- **Impact:** In production, transient API failures will crash the entire DAG run. With no retry, the pipeline requires manual re-triggering. The 5-second simulated latency could also cause upstream timeouts in constrained environments.
- **Mitigation:** Added exponential backoff retry (3 attempts, 1s/2s/4s delays), HTTP status code validation via `raise_for_status()`, and a 30-second request timeout.

---

### Issue 4: DROP+CREATE Pattern Destroys Staging Tables

- **Severity:** MEDIUM
- **File:** `scripts/extract_shipments.py`, lines 31–41; `scripts/extract_customer_tiers.py`, lines 28–36
- **Description:** Both extract scripts use `DROP TABLE IF EXISTS` followed by `CREATE TABLE` to reset staging. This means if the extraction fails mid-way (e.g., after dropping but before inserting all rows), the staging table is gone and downstream tasks will fail with a missing-table error.
- **Impact:** Partial failures leave the pipeline in an unrecoverable state without manual intervention. The transform step will error because its source table doesn't exist.
- **Mitigation:** Changed to `CREATE TABLE IF NOT EXISTS` + `TRUNCATE TABLE`, which atomically clears data while preserving the table structure. A mid-run failure still leaves a valid (empty) table, and the DAG retry can recover.

---

### Issue 5: No Idempotency in Analytics Loading

- **Severity:** HIGH
- **File:** `scripts/load_analytics.py`, lines 34–42
- **Description:** The load step does `INSERT INTO analytics.shipping_spend_by_tier` without clearing existing rows. Every re-run appends duplicate aggregated rows, so `SELECT SUM(total_shipping_spend)` returns 2x after the second run, 3x after the third, and so on.
- **Impact:** Analytics numbers grow with every pipeline execution. Business users would see inflated spend figures. This is the kind of bug that erodes trust in data — someone eventually notices the numbers don't match, and then nobody trusts the dashboard.
- **Mitigation:** Added `TRUNCATE TABLE analytics.shipping_spend_by_tier` before the INSERT. This is simpler and more predictable than an upsert for a full-refresh aggregation table.

---

### Issue 6: Duplicate Shipment Records

- **Severity:** MEDIUM
- **File:** `api/app.py`, lines 15 and 20
- **Description:** Shipment `SHP002` appears twice in the mock data with different costs ($45.00 and $47.00). The pipeline ingests both without deduplication, inflating CUST002's shipping spend.
- **Impact:** Double-counted shipments produce incorrect per-tier spend figures. In a real system, this could come from API pagination overlap, retry-on-timeout creating duplicates, or upstream data issues.
- **Mitigation:** Added deduplication in the transform step. For duplicate `shipment_id`s, I keep the row with the latest `loaded_at` timestamp (most recently ingested version). This follows a "last-write-wins" strategy that's appropriate for corrections.

---

### Issue 7: Bad Data — Negative Costs, Zero Costs, Null Customers, Cancelled Shipments

- **Severity:** MEDIUM
- **Files:** `api/app.py` (data), `scripts/transform_data.py` (missing filters)
- **Description:** The source data contains several problematic records that flow straight through to analytics:
  - `SHP012`: shipping_cost = -5.00 (negative)
  - `SHP013`: shipping_cost = 0.00 (zero — no revenue impact)
  - `SHP014`: customer_id = null (can't attribute to any tier)
  - `SHP017`: status = "cancelled" (shouldn't count as spend)
  - `SHP011`: customer_id = "CUST999" (not in customer_tiers.csv)
- **Impact:** Negative costs reduce the aggregate incorrectly. Null customer IDs break the join logic. Cancelled shipments inflate counts. These all silently corrupt the analytics output.
- **Mitigation:** Added data quality filters in the transform step: exclude negative/zero costs, null customer IDs, and cancelled shipments. Unknown customers (like CUST999) are given an 'Unknown' tier via the existing LEFT JOIN + COALESCE. Added logging of filtered-out records for observability.

---

### Issue 8: Duplicate Customer Tier (SCD Not Handled)

- **Severity:** MEDIUM
- **File:** `data/customer_tiers.csv`, rows 2 and 8
- **Description:** CUST002 has two tier entries — Platinum (effective 2024-01-01) and Gold (effective 2024-02-15). The current LEFT JOIN picks up both rows, creating duplicate shipments in the joined table.
- **Impact:** Every CUST002 shipment gets doubled in the transform output, once with Platinum and once with Gold. This leads to double-counting in analytics.
- **Mitigation:** Implemented SCD-aware (Slowly Changing Dimension) join logic in the transform step. Each shipment is matched to the customer tier that was effective on the shipment's date, using a correlated subquery to find the most recent tier_updated_date that's <= the shipment_date.

---

### Issue 9: Hardcoded Database Credentials

- **Severity:** MEDIUM
- **Files:** `scripts/extract_shipments.py` (lines 15–19), `scripts/extract_customer_tiers.py` (lines 14–18), `scripts/transform_data.py` (lines 13–17), `scripts/load_analytics.py` (lines 13–17), `docker-compose.yml` (lines 8–10, 46)
- **Description:** Database credentials (`airflow`/`airflow`) are hardcoded in every script and in the docker-compose file. There's no separation between configuration and code.
- **Impact:** Credentials in source code end up in version control. Changing the password requires editing 5+ files. In a real deployment, this would be a security finding in any audit.
- **Mitigation:** All scripts now read credentials from environment variables (`DB_HOST`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`) with sensible defaults for local development. docker-compose passes these via environment variables. Production deployments can override via `.env` files or secrets management.

---

### Issue 10: Insufficient DAG Retry Configuration

- **Severity:** LOW
- **File:** `dags/shipment_analytics_dag.py`, line 25
- **Description:** The DAG has `retries: 1` with a 1-minute delay. Given the API's simulated 10% failure rate and occasional 5-second latency spikes, a single retry is often not enough.
- **Impact:** DAG runs fail more frequently than necessary, requiring manual intervention to re-trigger.
- **Mitigation:** Increased retries to 3 with a 2-minute delay between attempts. Combined with the per-request retry logic in the extract script, this provides two layers of fault tolerance.

---

### Issue 11: No Database Constraints

- **Severity:** LOW
- **File:** `sql/init.sql`
- **Description:** The init script only creates schemas and grants permissions. There are no table definitions, constraints, or indexes. The staging tables are created on-the-fly by the extract scripts with no NOT NULL constraints, no CHECK constraints, and no indexes.
- **Impact:** Bad data (nulls, negatives) can enter the system unchecked at the database level. Queries on large tables without indexes would degrade performance. There's no defense-in-depth — the application code is the only barrier.
- **Mitigation:** Added table definitions in init.sql with NOT NULL constraints on critical fields, a CHECK constraint on `shipping_cost >= 0`, and indexes on `shipment_id`, `customer_id`, and `shipment_date`.

---

### Issue 12: No Tests

- **Severity:** MEDIUM
- **File:** `tests/test_sample.py`
- **Description:** The test directory contains only a sample test with `assert True`. There are no tests for any pipeline logic — no transform tests, no idempotency tests, no edge case coverage.
- **Impact:** Regressions go undetected. Refactoring is risky because there's no safety net. New contributors have no way to verify their changes don't break existing behavior.
- **Mitigation:** Created a comprehensive test suite: `test_extract_shipments.py` (retry logic, parameterized queries), `test_transform.py` (all filters, dedup, SCD joins), `test_load.py` (idempotency verification), and `conftest.py` with shared fixtures and mocks.

---

## Summary Table

| # | Issue | Severity | Status |
|---|-------|----------|--------|
| 1 | Port conflict | HIGH | Fixed |
| 2 | SQL injection | HIGH | Fixed |
| 3 | No API error handling | HIGH | Fixed |
| 4 | DROP+CREATE pattern | MEDIUM | Fixed |
| 5 | No idempotency | HIGH | Fixed |
| 6 | Duplicate shipments | MEDIUM | Fixed |
| 7 | Bad data not filtered | MEDIUM | Fixed |
| 8 | SCD not handled | MEDIUM | Fixed |
| 9 | Hardcoded credentials | MEDIUM | Fixed |
| 10 | Insufficient retries | LOW | Fixed |
| 11 | No DB constraints | LOW | Fixed |
| 12 | No tests | MEDIUM | Fixed |
