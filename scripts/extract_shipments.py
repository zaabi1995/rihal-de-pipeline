"""
Extract shipment data from the external API
"""
import os
import time
import logging
import requests
import psycopg2
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration from environment
DB_HOST = os.environ.get('DB_HOST', 'postgres')
DB_NAME = os.environ.get('DB_NAME', 'airflow')
DB_USER = os.environ.get('DB_USER', 'airflow')
DB_PASSWORD = os.environ.get('DB_PASSWORD', 'airflow')
API_BASE_URL = os.environ.get('API_BASE_URL', 'http://api:8000')

MAX_RETRIES = 3
RETRY_BACKOFF = 1  # seconds, doubles each retry


def get_db_connection():
    """Create and return a database connection."""
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )


def fetch_shipments_with_retry():
    """
    Fetch shipment data from the API with exponential backoff retry.
    Returns the list of shipments on success, raises on exhausted retries.
    """
    url = f"{API_BASE_URL}/api/shipments"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"API request attempt {attempt}/{MAX_RETRIES}")
            response = requests.get(url, timeout=30)
            response.raise_for_status()

            data = response.json()
            shipments = data['data']
            logger.info(f"Fetched {len(shipments)} shipments from API")
            return shipments

        except (requests.exceptions.RequestException, KeyError, ValueError) as e:
            logger.warning(f"Attempt {attempt} failed: {e}")
            if attempt < MAX_RETRIES:
                wait_time = RETRY_BACKOFF * (2 ** (attempt - 1))
                logger.info(f"Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                logger.error("All retry attempts exhausted")
                raise


def extract_shipments_from_api():
    """
    Fetch shipment data from the external API and load into staging table.
    Uses parameterized queries to prevent SQL injection.
    Uses TRUNCATE instead of DROP+CREATE for safe re-runs.
    """
    logger.info("Starting shipment data extraction...")

    shipments = fetch_shipments_with_retry()

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Create table if it doesn't exist, then truncate for idempotency
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS staging.shipments (
                shipment_id VARCHAR(50),
                customer_id VARCHAR(50),
                shipping_cost DECIMAL(10,2),
                shipment_date DATE,
                status VARCHAR(50),
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cursor.execute("TRUNCATE TABLE staging.shipments;")

        # Load data using parameterized queries (no SQL injection)
        insert_query = """
            INSERT INTO staging.shipments (shipment_id, customer_id, shipping_cost, shipment_date, status)
            VALUES (%s, %s, %s, %s, %s);
        """
        for shipment in shipments:
            cursor.execute(insert_query, (
                shipment['shipment_id'],
                shipment.get('customer_id'),  # .get() handles null gracefully
                shipment['shipping_cost'],
                shipment['shipment_date'],
                shipment['status']
            ))

        conn.commit()
        logger.info(f"Loaded {len(shipments)} shipments into staging")

    except Exception as e:
        conn.rollback()
        logger.error(f"Extraction failed: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

    logger.info("Shipment data extraction completed")
