"""
Load analytics data - calculate total shipping spend per customer tier per month
"""
import psycopg2
from datetime import datetime

def load_analytics_data():
    """
    Calculate and load final analytics: Total Shipping Spend per Customer Tier per Month
    """
    print("Starting analytics data load...")
    
    # Connect to database
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()
    
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
    
    # Calculate and load analytics
    cursor.execute("""
        INSERT INTO analytics.shipping_spend_by_tier (tier, year_month, total_shipping_spend, shipment_count)
        SELECT 
            COALESCE(tier, 'Unknown') as tier,
            TO_CHAR(shipment_date, 'YYYY-MM') as year_month,
            SUM(shipping_cost) as total_shipping_spend,
            COUNT(*) as shipment_count
        FROM staging.shipments_with_tiers
        GROUP BY tier, TO_CHAR(shipment_date, 'YYYY-MM');
    """)
    
    rows_inserted = cursor.rowcount
    print(f"Inserted {rows_inserted} rows into analytics table")
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print("Analytics data load completed")
