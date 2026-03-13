"""
Extract customer tier data from CSV file
"""
import os
import logging
import pandas as pd
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


def extract_customer_tiers_from_csv():
    """
    Load customer tier data from CSV file into staging table.
    Validates rows before insertion and uses TRUNCATE for safe re-runs.
    """
    logger.info("Starting customer tier extraction...")

    # Load CSV file
    df = pd.read_csv('/opt/airflow/data/customer_tiers.csv')
    logger.info(f"Loaded {len(df)} rows from CSV")

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Create table if it doesn't exist, then truncate for idempotency
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS staging.customer_tiers (
                customer_id VARCHAR(50),
                customer_name VARCHAR(200),
                tier VARCHAR(50),
                tier_updated_date DATE
            );
        """)
        cursor.execute("TRUNCATE TABLE staging.customer_tiers;")

        # Load data with validation
        skipped = 0
        loaded = 0
        for _, row in df.iterrows():
            # Skip rows with missing critical fields
            if pd.isna(row['customer_id']) or pd.isna(row['tier']):
                logger.warning(f"Skipping invalid row: {row.to_dict()}")
                skipped += 1
                continue

            cursor.execute("""
                INSERT INTO staging.customer_tiers (customer_id, customer_name, tier, tier_updated_date)
                VALUES (%s, %s, %s, %s);
            """, (row['customer_id'], row['customer_name'], row['tier'], row['tier_updated_date']))
            loaded += 1

        conn.commit()
        logger.info(f"Loaded {loaded} rows, skipped {skipped} invalid rows")

    except Exception as e:
        conn.rollback()
        logger.error(f"Extraction failed: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

    logger.info("Customer tier extraction completed")
