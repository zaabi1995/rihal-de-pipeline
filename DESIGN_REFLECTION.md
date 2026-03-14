# Design Reflection

## Critical Issues

### What was the most critical issue you discovered?

The SQL injection vulnerability in `extract_shipments.py` was the most dangerous issue. The original code used Python f-strings to interpolate API response data directly into SQL:

```python
cursor.execute(f"""
    INSERT INTO staging.shipments ...
    VALUES ('{shipment['shipment_id']}', '{shipment['customer_id']}', ...)
""")
```

This means whatever the upstream API returns ends up as executable SQL. If the API were compromised (or even if a legitimate shipment ID contained a single quote), an attacker could craft a payload like:

```
shipment_id: "'; DROP TABLE staging.shipments; --"
```

That would terminate the INSERT, drop the staging table, and comment out the rest. Worse, they could use `COPY TO` or `pg_read_file()` to exfiltrate data from the database — including Airflow's own metadata (connection strings, user credentials, DAG configurations).

The fix was straightforward: parameterized queries with `%s` placeholders. psycopg2 handles escaping, type conversion, and NULL handling automatically. This is one of those issues where the fix is simple but the potential impact is severe — exactly the kind of thing that should be caught in code review.

What made it particularly concerning is that `extract_customer_tiers.py` in the same codebase already used parameterized queries correctly. The inconsistency suggests it was an oversight, not a deliberate choice.

---

## Trade-offs

### What trade-offs did you make?

**TRUNCATE+INSERT vs. Incremental Upsert**

I chose TRUNCATE+INSERT (full refresh) over an incremental upsert pattern for the analytics table. Here's why:

- The analytics table is an aggregation — it's derived entirely from staging data. There's no user-facing state to preserve, no partial updates to merge. Full refresh is simpler to reason about: every run produces the same output from the same input, period.
- An upsert (ON CONFLICT DO UPDATE) would require a unique constraint on (tier, year_month), which adds complexity for a table that's only ~10-20 rows. The marginal performance gain is meaningless at this scale.
- The trade-off: if the pipeline ever runs in a high-concurrency environment where another process reads analytics while we're reloading, there's a brief window where the table is empty. For a daily batch pipeline, this is fine. For a real-time system, I'd use a swap pattern (write to a temp table, then rename).

**SQL-based Transform vs. Python/Pandas**

I kept the transform logic in SQL rather than pulling data into Python with pandas. SQL is more efficient for set-based operations (JOINs, GROUP BY, DISTINCT ON), and it keeps the data in the database rather than serializing it over the wire. The trade-off is that the SQL is harder to unit-test — I can verify the SQL text contains the right clauses, but I can't easily run it in a test without a real PostgreSQL instance. I supplemented with Python-based data quality tests that validate the same rules independently.

**Last-Write-Wins Dedup**

For duplicate shipment IDs (SHP002 appears twice with different costs), I chose last-write-wins using `DISTINCT ON ... ORDER BY loaded_at DESC`. This assumes the most recently loaded version is the correction. An alternative would be to take the average, or flag duplicates for manual review. I chose last-write-wins because it's a common pattern for handling corrections in ETL pipelines, and it doesn't require human intervention.

---

## Failure Scenarios

### Where could your solution still fail?

1. **API schema changes.** If the API adds, removes, or renames a field, the extraction will either crash (KeyError) or silently insert NULLs. I don't validate the response schema — a JSON Schema check or Pydantic model would help.

2. **Concurrent DAG runs.** If two DAG runs overlap (e.g., a manual trigger while the daily schedule is running), both will TRUNCATE the same staging tables. This could cause one run to see an empty table. Airflow's `max_active_runs=1` (default) mitigates this, but it's not explicitly set.

3. **PostgreSQL connection exhaustion.** Each task opens its own connection. Under high parallelism or if connections aren't properly released (e.g., an exception before `conn.close()`), the database could run out of connections. I added try/finally blocks, but a connection pool would be better at scale.

4. **CSV file corruption.** If `customer_tiers.csv` is malformed (wrong delimiter, encoding issues, missing headers), pandas will either crash or parse incorrectly. There's basic row validation but no file-level integrity check.

5. **Clock skew in SCD join.** The tier matching uses `tier_updated_date <= shipment_date`. If tier updates are backdated or shipment dates are in the future, the join could pick the wrong tier. This is inherent to the SCD Type 2 pattern — it relies on consistent date semantics across data sources.

---

## Scalability

### How would this system behave at 100x data volume?

At 100x the current volume (~2,100 shipments instead of 21), the system would start hitting several bottlenecks:

**API extraction:** The current approach fetches all shipments in a single request. At 100x, this means a large JSON payload that could timeout or exceed memory. I'd implement pagination (the API already supports `start_date`/`end_date` filters) and process shipments in batches of 1,000.

**Row-by-row inserts:** Both extract scripts insert one row at a time. At 100x, that's ~2,100 individual INSERT statements. PostgreSQL can handle this, but it's inefficient. I'd switch to `executemany()` or `COPY FROM` with a CSV buffer for bulk loading — easily 10-50x faster.

**Transform query:** The `LATERAL` subquery join is O(n*m) in the worst case (each shipment scans the tier table). With indexes on `customer_id` and `tier_updated_date`, this stays fast. At truly large scale (millions of rows), I'd materialize the SCD mapping into a lookup table first.

**Analytics aggregation:** The GROUP BY is straightforward and PostgreSQL handles it efficiently with indexes. Even at 100x, a few thousand rows grouped by tier and month is trivial.

**Infrastructure:** At 100x, I'd also consider:
- Table partitioning by `shipment_date` (monthly partitions) for faster queries and easier data lifecycle management
- Connection pooling (PgBouncer) to avoid connection storms
- Airflow KubernetesExecutor for horizontal scaling of workers

---

## Future Improvements

### What would you redesign with more time?

1. **Streaming ingestion.** Replace the batch pull from the API with a webhook or message queue (Kafka, RabbitMQ). Shipments would arrive as events, reducing latency from "once per day" to seconds. The staging tables would become append-only event logs.

2. **Schema versioning.** Add Alembic or Flyway for database migrations. Right now, schema changes require editing `init.sql` and recreating the database. In production, you need backward-compatible migrations that can run without downtime.

3. **Monitoring and alerting.** Add data quality assertions with tools like Great Expectations or dbt tests. Alert when: zero rows extracted, more than 5% of rows filtered, analytics totals deviate >10% from the previous run. Push alerts to Slack or PagerDuty.

4. **Data lineage.** Track which source rows contributed to each analytics row. If a business user questions a number, you should be able to trace it back to specific shipments. Tools like OpenLineage with Airflow integration can automate this.

5. **Secrets management.** Move from environment variables to Airflow Connections or a vault (HashiCorp Vault, AWS Secrets Manager). Environment variables work for development, but in production you want rotation, auditing, and least-privilege access.

---

## Deep Dives

### Explain one complex function line-by-line

I'll walk through the `transform_shipment_data()` function's main SQL query, since it's the most complex piece of logic in the pipeline.

```python
def transform_shipment_data():
```

The function orchestrates data quality filtering, deduplication, and SCD-aware joining — all in a single SQL statement using CTEs (Common Table Expressions).

First, I log data quality stats by running COUNT queries against the raw staging data. This gives observability into how many rows are being filtered and why, without affecting the transform itself.

Then the main query:

```sql
CREATE TABLE staging.shipments_with_tiers AS
```
Creates the output table directly from the query result. This is a full refresh — the table is dropped first (via `DROP TABLE IF EXISTS` earlier in the function) and recreated from scratch.

```sql
WITH deduped AS (
    SELECT DISTINCT ON (shipment_id)
        shipment_id, customer_id, shipping_cost, shipment_date, status, loaded_at
    FROM staging.shipments
    ORDER BY shipment_id, loaded_at DESC
),
```
**CTE 1: Deduplication.** `DISTINCT ON (shipment_id)` is a PostgreSQL extension that keeps only the first row for each unique `shipment_id`. Combined with `ORDER BY shipment_id, loaded_at DESC`, it keeps the most recently loaded version. This handles the SHP002 duplicate — we keep the $47.00 version (loaded second) and discard the $45.00 one.

```sql
clean AS (
    SELECT *
    FROM deduped
    WHERE customer_id IS NOT NULL
      AND shipping_cost > 0
      AND status != 'cancelled'
)
```
**CTE 2: Data quality filters.** Three filters, each targeting a specific data issue:
- `customer_id IS NOT NULL` — removes SHP014 (can't join to a tier without a customer)
- `shipping_cost > 0` — removes SHP012 (negative) and SHP013 (zero)
- `status != 'cancelled'` — removes SHP017 (cancelled shipments aren't real spend)

```sql
SELECT
    c.shipment_id, c.customer_id, c.shipping_cost, c.shipment_date, c.status,
    COALESCE(t.tier, 'Unknown') AS tier,
    COALESCE(t.customer_name, 'Unknown') AS customer_name
FROM clean c
LEFT JOIN LATERAL (
    SELECT tier, customer_name
    FROM staging.customer_tiers ct
    WHERE ct.customer_id = c.customer_id
      AND ct.tier_updated_date <= c.shipment_date
    ORDER BY ct.tier_updated_date DESC
    LIMIT 1
) t ON true;
```
**Final SELECT: SCD-aware join.** This is the most nuanced part. Instead of a regular JOIN (which would duplicate rows when a customer has multiple tier records), I use `LEFT JOIN LATERAL`:

- For each clean shipment, the subquery finds all tier records for that customer where the effective date is on or before the shipment date
- `ORDER BY tier_updated_date DESC LIMIT 1` picks the most recent applicable tier
- For CUST002 with a January shipment, it picks "Platinum" (effective Jan 1). For a March shipment, it picks "Gold" (effective Feb 15)
- `COALESCE(t.tier, 'Unknown')` handles customers with no tier record at all (like CUST999)

This approach correctly implements SCD Type 2 lookup without requiring an explicit date-range model in the source data.

### Explain one SQL transformation step-by-step

The analytics aggregation query in `load_analytics.py`:

```sql
INSERT INTO analytics.shipping_spend_by_tier (tier, year_month, total_shipping_spend, shipment_count)
SELECT
    tier,
    TO_CHAR(shipment_date, 'YYYY-MM') AS year_month,
    SUM(shipping_cost) AS total_shipping_spend,
    COUNT(*) AS shipment_count
FROM staging.shipments_with_tiers
GROUP BY tier, TO_CHAR(shipment_date, 'YYYY-MM');
```

Step by step:

1. **Source:** Reads from `staging.shipments_with_tiers` — the already-cleaned, deduplicated, tier-joined dataset.

2. **`TO_CHAR(shipment_date, 'YYYY-MM')`** — Converts each shipment's date to a year-month string (e.g., "2024-01" for January 2024). This is the time granularity for our analytics — we're aggregating by month, not by day or week.

3. **`GROUP BY tier, TO_CHAR(shipment_date, 'YYYY-MM')`** — Groups all shipments that share the same customer tier AND the same month. So "Gold customers in January 2024" is one group, "Gold customers in February 2024" is another.

4. **`SUM(shipping_cost)`** — Within each group, adds up all shipping costs. This gives us the total shipping spend for that tier in that month.

5. **`COUNT(*)`** — Counts how many shipments are in each group. This gives context to the spend figure — $100 total spend from 2 shipments is different from $100 from 50 shipments.

6. **`INSERT INTO analytics.shipping_spend_by_tier`** — Writes the aggregated results directly into the analytics table. Because we TRUNCATE before this INSERT, the table always reflects the current state of the transform output.

The result is a compact summary table: one row per (tier, month) combination, showing total spend and shipment count. This is what business users would query for dashboards or reports.

### Describe one alternative design you considered and rejected

**Alternative: Using Airflow XCom to pass data between tasks**

Instead of using PostgreSQL staging tables as the intermediary between pipeline stages, I considered using Airflow's XCom (cross-communication) mechanism. In this approach:

- The extract task would fetch data and push it to XCom as a JSON-serialized list
- The transform task would pull data from XCom, apply filters in Python, and push the result back
- The load task would pull the transformed data and insert it into the analytics table

**Why I considered it:** XCom eliminates the need for staging tables entirely. The data lives in Airflow's metadata database, and there's no risk of staging table conflicts between concurrent runs. The transform logic would be pure Python, making it much easier to unit-test.

**Why I rejected it:**

1. **XCom has a size limit.** By default, XCom stores data in Airflow's metadata database (the same PostgreSQL instance). Values are limited to ~48KB in the default serializer. Even with a custom serializer, pushing hundreds of thousands of shipments through XCom is asking for trouble — it would bloat the metadata DB and slow down the Airflow scheduler.

2. **No SQL-level data integrity.** With staging tables, I can add constraints, indexes, and use the database's own transaction isolation. With XCom, data validation is purely application-level — one bug in the Python code and corrupt data flows through silently.

3. **Debugging is harder.** When something goes wrong with staging tables, I can `SELECT * FROM staging.shipments` and see exactly what was extracted. With XCom, I'd need to dig through the Airflow UI or query the metadata database's `xcom` table, which stores serialized blobs.

4. **The staging pattern is industry-standard.** ETL pipelines conventionally use staging areas in the database. It's well-understood, scales predictably, and makes the pipeline's state observable to anyone with SQL access — not just Airflow operators.

The trade-off: staging tables add infrastructure coupling (the pipeline depends on PostgreSQL being available between tasks), and they require explicit cleanup. But for a batch ETL pipeline processing structured tabular data, the reliability and observability benefits of database staging far outweigh the simplicity of XCom.
