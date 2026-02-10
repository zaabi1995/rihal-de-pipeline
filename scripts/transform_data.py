"""
Transform shipment data by joining with customer tiers
"""
import psycopg2

def transform_shipment_data():
    """
    Join shipment data with customer tier data and prepare for analytics
    """
    print("Starting data transformation...")
    
    # Connect to database
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()
    
    # Create combined view
    cursor.execute("DROP TABLE IF EXISTS staging.shipments_with_tiers;")
    
    # Join shipments with customer tier information
    cursor.execute("""
        CREATE TABLE staging.shipments_with_tiers AS
        SELECT 
            s.shipment_id,
            s.customer_id,
            s.shipping_cost,
            s.shipment_date,
            s.status,
            t.tier,
            t.customer_name
        FROM staging.shipments s
        LEFT JOIN staging.customer_tiers t 
            ON s.customer_id = t.customer_id;
    """)
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print("Data transformation completed")
