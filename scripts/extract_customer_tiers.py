"""
Extract customer tier data from CSV file
"""
import pandas as pd
import psycopg2

def extract_customer_tiers_from_csv():
    """
    Load customer tier data from CSV file into database
    """
    print("Starting customer tier extraction...")
    
    # Connect to database
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()
    
    # Load CSV file
    df = pd.read_csv('/opt/airflow/data/customer_tiers.csv')
    
    print(f"Loaded {len(df)} rows from CSV")
    
    # Create staging table
    cursor.execute("DROP TABLE IF EXISTS staging.customer_tiers;")
    cursor.execute("""
        CREATE TABLE staging.customer_tiers (
            customer_id VARCHAR(50),
            customer_name VARCHAR(200),
            tier VARCHAR(50),
            tier_updated_date DATE
        );
    """)
    
    # Load data into staging
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO staging.customer_tiers (customer_id, customer_name, tier, tier_updated_date)
            VALUES (%s, %s, %s, %s);
        """, (row['customer_id'], row['customer_name'], row['tier'], row['tier_updated_date']))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print("Customer tier extraction completed")
