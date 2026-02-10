"""
Extract shipment data from the external API
"""
import requests
import psycopg2
from datetime import datetime

def extract_shipments_from_api():
    """
    Fetch shipment data from the external API and load into database
    """
    print("Starting shipment data extraction...")
    
    # Connect to database
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()
    
    # Fetch data from external API
    response = requests.get("http://api:8000/api/shipments")
    data = response.json()
    shipments = data['data']
    
    print(f"Fetched {len(shipments)} shipments from API")
    
    # Create staging table
    cursor.execute("DROP TABLE IF EXISTS staging.shipments;")
    cursor.execute("""
        CREATE TABLE staging.shipments (
            shipment_id VARCHAR(50),
            customer_id VARCHAR(50),
            shipping_cost DECIMAL(10,2),
            shipment_date DATE,
            status VARCHAR(50),
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    # Load data into staging
    for shipment in shipments:
        cursor.execute(f"""
            INSERT INTO staging.shipments (shipment_id, customer_id, shipping_cost, shipment_date, status)
            VALUES ('{shipment['shipment_id']}', '{shipment['customer_id']}', 
                    {shipment['shipping_cost']}, '{shipment['shipment_date']}', '{shipment['status']}');
        """)
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print("Shipment data extraction completed")
