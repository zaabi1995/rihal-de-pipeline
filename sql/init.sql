-- Database initialization script
-- Creates schemas, staging tables, analytics tables, constraints, and indexes

-- Create schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA staging TO airflow;
GRANT ALL PRIVILEGES ON SCHEMA analytics TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA analytics TO airflow;

-- Set default privileges
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT ALL ON TABLES TO airflow;

-- Staging: shipments
CREATE TABLE IF NOT EXISTS staging.shipments (
    shipment_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50),
    shipping_cost DECIMAL(10,2) NOT NULL,
    shipment_date DATE NOT NULL,
    status VARCHAR(50) NOT NULL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_shipments_shipment_id ON staging.shipments (shipment_id);
CREATE INDEX IF NOT EXISTS idx_shipments_customer_id ON staging.shipments (customer_id);
CREATE INDEX IF NOT EXISTS idx_shipments_date ON staging.shipments (shipment_date);

-- Staging: customer tiers
CREATE TABLE IF NOT EXISTS staging.customer_tiers (
    customer_id VARCHAR(50) NOT NULL,
    customer_name VARCHAR(200),
    tier VARCHAR(50) NOT NULL,
    tier_updated_date DATE NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tiers_customer_id ON staging.customer_tiers (customer_id);
CREATE INDEX IF NOT EXISTS idx_tiers_updated_date ON staging.customer_tiers (tier_updated_date);

-- Analytics: shipping spend by tier
CREATE TABLE IF NOT EXISTS analytics.shipping_spend_by_tier (
    tier VARCHAR(50) NOT NULL,
    year_month VARCHAR(7) NOT NULL,
    total_shipping_spend DECIMAL(12,2) NOT NULL CHECK (total_shipping_spend >= 0),
    shipment_count INTEGER NOT NULL CHECK (shipment_count > 0),
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_analytics_tier ON analytics.shipping_spend_by_tier (tier);
CREATE INDEX IF NOT EXISTS idx_analytics_year_month ON analytics.shipping_spend_by_tier (year_month);
