"""
Load analytics data - calculate total shipping spend per customer tier per month.
Idempotent: TRUNCATE before INSERT ensures re-runs produce the same result.
"""
import os
import logging
import psycopg2
from datetime import datetime

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


def load_analytics_data():
    """
    Calculate and load final analytics: Total Shipping Spend per Customer Tier per Month.

    Idempotency strategy: TRUNCATE the analytics table before inserting.
    This is appropriate for a full-refresh aggregation — the entire result set
    is recomputed from the transform output every run. Simpler and more
    predictable than an upsert for this use case.
    """
    logger.info("Starting analytics data load...")

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Create analytics table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS analytics.shipping_spend_by_tier (
                tier VARCHAR(50),
                year_month VARCHAR(7),
                total_shipping_spend DECIMAL(12,2),
                shipment_count INTEGER,
                calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # TRUNCATE before INSERT for idempotency — re-runs won't duplicate rows
        cursor.execute("TRUNCATE TABLE analytics.shipping_spend_by_tier;")
        logger.info("Truncated analytics table for clean reload")

        # Calculate and load analytics
        cursor.execute("""
            INSERT INTO analytics.shipping_spend_by_tier (tier, year_month, total_shipping_spend, shipment_count)
            SELECT
                tier,
                TO_CHAR(shipment_date, 'YYYY-MM') AS year_month,
                SUM(shipping_cost) AS total_shipping_spend,
                COUNT(*) AS shipment_count
            FROM staging.shipments_with_tiers
            GROUP BY tier, TO_CHAR(shipment_date, 'YYYY-MM');
        """)

        rows_inserted = cursor.rowcount
        logger.info(f"Inserted {rows_inserted} rows into analytics table")

        # Verification: read back the totals
        cursor.execute("""
            SELECT COUNT(*), SUM(total_shipping_spend), SUM(shipment_count)
            FROM analytics.shipping_spend_by_tier;
        """)
        row_count, total_spend, total_shipments = cursor.fetchone()
        logger.info(f"Verification — rows: {row_count}, total spend: {total_spend}, total shipments: {total_shipments}")

        conn.commit()

    except Exception as e:
        conn.rollback()
        logger.error(f"Analytics load failed: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

    logger.info("Analytics data load completed")
