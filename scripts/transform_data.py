"""
Transform shipment data by joining with customer tiers.
Handles: deduplication, data quality filtering, SCD-aware tier matching.
"""
import os
import logging
import psycopg2

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration from environment
DB_HOST = os.environ.get('DB_HOST', 'postgres')
DB_NAME = os.environ.get('DB_NAME', 'airflow')
DB_USER = os.environ.get('DB_USER', 'airflow')
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'airflow')


def get_db_connection():
    """Create and return a database connection."""
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


def transform_shipment_data():
    """
    Join shipment data with customer tier data and prepare for analytics.

    Data quality rules applied:
    1. Remove shipments with null customer_id (can't attribute to a tier)
    2. Remove shipments with negative or zero shipping_cost (invalid data)
    3. Remove cancelled shipments (not actual spend)
    4. Deduplicate by shipment_id (keep the most recently loaded version)
    5. SCD-aware join: match each shipment to the customer tier effective on that date
    6. Unknown customers (no tier match) get tier = 'Unknown'

    Each filter is logged with row counts for observability.
    """
    logger.info("Starting data transformation...")

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # --- Data Quality Stats ---
        cursor.execute("SELECT COUNT(*) FROM staging.shipments;")
        total_raw = cursor.fetchone()[0]
        logger.info(f"Raw shipments in staging: {total_raw}")

        cursor.execute("SELECT COUNT(*) FROM staging.shipments WHERE customer_id IS NULL;")
        null_customers = cursor.fetchone()[0]
        logger.info(f"Shipments with null customer_id (filtered): {null_customers}")

        cursor.execute("SELECT COUNT(*) FROM staging.shipments WHERE shipping_cost <= 0;")
        bad_costs = cursor.fetchone()[0]
        logger.info(f"Shipments with non-positive cost (filtered): {bad_costs}")

        cursor.execute("SELECT COUNT(*) FROM staging.shipments WHERE status = 'cancelled';")
        cancelled = cursor.fetchone()[0]
        logger.info(f"Cancelled shipments (filtered): {cancelled}")

        # Count duplicates (shipment_ids appearing more than once)
        cursor.execute("""
            SELECT COUNT(*) FROM (
                SELECT shipment_id FROM staging.shipments
                GROUP BY shipment_id HAVING COUNT(*) > 1
            ) dups;
        """)
        dup_ids = cursor.fetchone()[0]
        logger.info(f"Duplicate shipment_id groups (deduped): {dup_ids}")

        # --- Create transformed table ---
        cursor.execute("DROP TABLE IF EXISTS staging.shipments_with_tiers;")

        # The query below does the following in order:
        # 1. CTE "deduped": removes duplicates by keeping the latest loaded_at per shipment_id
        # 2. CTE "clean": filters out null customer_id, non-positive costs, cancelled status
        # 3. CTE "tier_ranked": for each customer, ranks tiers by effective date
        # 4. Final SELECT: joins clean shipments to the correct tier using SCD logic
        #    - For each shipment, find the tier where tier_updated_date <= shipment_date
        #    - If no tier matches (new customer or future-dated tier), use 'Unknown'
        cursor.execute("""
            CREATE TABLE staging.shipments_with_tiers AS
            WITH deduped AS (
                SELECT DISTINCT ON (shipment_id)
                    shipment_id, customer_id, shipping_cost, shipment_date, status, loaded_at
                FROM staging.shipments
                ORDER BY shipment_id, loaded_at DESC
            ),
            clean AS (
                SELECT *
                FROM deduped
                WHERE customer_id IS NOT NULL
                  AND shipping_cost > 0
                  AND status != 'cancelled'
            )
            SELECT
                c.shipment_id,
                c.customer_id,
                c.shipping_cost,
                c.shipment_date,
                c.status,
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
        """)

        cursor.execute("SELECT COUNT(*) FROM staging.shipments_with_tiers;")
        final_count = cursor.fetchone()[0]
        logger.info(f"Transformed shipments (after all filters): {final_count}")

        conn.commit()

    except Exception as e:
        conn.rollback()
        logger.error(f"Transformation failed: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

    logger.info("Data transformation completed")
