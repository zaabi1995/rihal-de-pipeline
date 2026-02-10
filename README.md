# 🏗 The 2026 Rihal Data Engineering Talent Challenge

Welcome to the Rihal Data Engineering Challenge! You have inherited a partially implemented data pipeline that processes shipment data for internal reporting.

## 📋 Challenge Overview

Your task is to **audit, improve, and harden** this data pipeline to make it production-ready. The system currently runs, but should not be assumed to be correct, reliable, or scalable.

## 🚀 Quick Start

### Prerequisites
- Docker Desktop installed and running
- Docker Compose
- At least 4GB of available RAM

### Setup Instructions

1. **Clone or extract this repository**

2. **Start the services**
   ```bash
   docker-compose up -d
   ```

3. **Wait for services to initialize** (about 2-3 minutes)
   - Airflow UI will be available at: http://localhost:8080
   - Mock API at: http://localhost:8000
   - PostgreSQL at: localhost:5432

4. **Access Airflow**
   - URL: http://localhost:8080
   - Username: `admin`
   - Password: `admin`

5. **Run the pipeline**
   - Navigate to the DAG: `shipment_analytics_pipeline`
   - Enable the DAG (toggle switch on the left)
   - Click the "Play" button to trigger a manual run

6. **Check the results**
   ```bash
   docker-compose exec postgres psql -U airflow -d airflow -c "SELECT * FROM analytics.shipping_spend_by_tier ORDER BY year_month, tier;"
   ```

### Stopping the Services

```bash
docker-compose down
```

To remove all data and start fresh:
```bash
docker-compose down -v
```

## 📁 Project Structure

```
.
├── dags/                          # Airflow DAG definitions
│   └── shipment_analytics_dag.py
├── scripts/                       # Pipeline scripts
│   ├── extract_shipments.py      # Extract from API
│   ├── extract_customer_tiers.py # Extract from CSV
│   ├── transform_data.py         # Transform and join
│   └── load_analytics.py         # Load analytics
├── sql/                           # SQL scripts
│   └── init.sql                  # Database initialization
├── data/                          # Source data files
│   └── customer_tiers.csv
├── api/                           # Mock external API
│   ├── app.py
│   └── Dockerfile
├── tests/                         # Your tests go here
├── docker-compose.yml
├── Dockerfile
└── README.md
```

## 📊 Data Flow

1. **Extract Shipment Data** - Fetch from REST API (`http://api:8000/api/shipments`)
2. **Extract Customer Tiers** - Load from CSV file
3. **Transform** - Join shipments with customer tier information
4. **Load Analytics** - Calculate total shipping spend per customer tier per month

## 🎯 Your Responsibilities

### Task 1: Engineering Audit
Create `ENGINEERING_AUDIT.md` documenting:
- Critical weaknesses in the current system
- Severity ranking (High/Medium/Low)
- Potential production impact
- How you mitigated each issue

### Task 2: Improve the Pipeline
Make the pipeline production-ready by demonstrating engineering best practices:
- Data Integrity & Consistency
- Fault Tolerance & Resilience
- Security & Compliance
- Maintainability & Observability
- Scalability Considerations

### Task 3: Data Integrity & Modeling
Ensure correct analytical output:
- **Total Shipping Spend per Customer Tier per Month**

Requirements:
- Logical data modeling
- Correct handling of conflicting/duplicate records
- Clear transformation boundaries
- Consistent aggregation logic

### Task 4: Testing (Mandatory)
Create meaningful tests in the `tests/` folder that validate:
- Core transformation logic behaves as expected
- Rerunning the pipeline does not corrupt results
- Critical edge cases are handled correctly

### Task 5: Design Reflection
Create `DESIGN_REFLECTION.md` answering:
- What was the most critical issue you discovered?
- What trade-offs did you make?
- Where could your solution still fail?
- How would this system behave at 100x data volume?
- What would you redesign with more time?

Plus:
- Explain one complex function line-by-line
- Explain one SQL transformation step-by-step
- Describe one alternative design you considered and rejected

## 📤 Submission Requirements

Your submission must include:
- [ ] All code runs via `docker-compose up`
- [ ] No manual database intervention required
- [ ] No hardcoded credentials
- [ ] `ENGINEERING_AUDIT.md`
- [ ] `DESIGN_REFLECTION.md`
- [ ] Meaningful tests in `tests/`
- [ ] Updated `README.md` with any new setup instructions

## ⚙️ Technical Stack

**Required Technologies:**
- Apache Airflow
- Docker & docker-compose
- Python 3.9+
- PostgreSQL
- SQL

You may restructure or refactor as needed, but these core technologies must remain.

## 🔍 Useful Commands

### View Logs
```bash
# Airflow logs
docker-compose logs airflow-scheduler -f

# API logs
docker-compose logs api -f

# Database logs
docker-compose logs postgres -f
```

### Connect to Database
```bash
docker-compose exec postgres psql -U airflow -d airflow
```

### Check API
```bash
curl http://localhost:8000/api/shipments | jq
```

### Restart Services
```bash
docker-compose restart airflow-scheduler airflow-webserver
```

## 💡 Tips

- Start by running the pipeline and observing its behavior
- Check what data ends up in the database
- Try running the pipeline multiple times
- Look for edge cases in the source data
- Consider what could go wrong in production
- Think about data quality and consistency

## 📞 Support

If you encounter issues with the Docker setup or have questions about the challenge requirements, please contact the Rihal recruitment team.

---

**Good luck! We're excited to see your approach to improving this system.**
