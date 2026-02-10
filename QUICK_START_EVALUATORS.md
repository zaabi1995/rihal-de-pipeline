# 🚀 Quick Start - Rihal Challenge for Evaluators

> ⚠️ **FOR EVALUATORS ONLY - DO NOT SHARE WITH CANDIDATES**  
> This document contains spoilers about all intentional flaws in the challenge.

## Testing the Challenge

### 1. Start Services
```bash
cd c:\Users\AhmedAlSaqqaf\.gemini\antigravity\scratch\Codestacker-DE-2026
docker-compose up -d
```

### 2. Access Airflow
- URL: http://localhost:8080
- User: admin / admin
- Wait 2-3 minutes for initialization

### 3. Run Pipeline
1. Find DAG: `shipment_analytics_pipeline`
2. Enable it (toggle switch)
3. Trigger manually (play button)

### 4. Verify Results
```bash
docker-compose exec postgres psql -U airflow -d airflow
```

```sql
-- Check results
SELECT * FROM analytics.shipping_spend_by_tier 
ORDER BY year_month, tier;

-- Expected: 4 rows (Bronze, Gold, Platinum, Silver across months)
```

### 5. **TEST IDEMPOTENCY BUG** 🐛
Trigger the DAG again and check results:

```sql
SELECT * FROM analytics.shipping_spend_by_tier 
ORDER BY year_month, tier;

-- Results will DOUBLE! This is the critical bug.
```

### 6. Stop Services
```bash
docker-compose down
docker-compose down -v  # Remove data
```

---

## 📊 Grading Quick Reference

| Score | Level | Key Indicators |
|-------|-------|---------------|
| 90-100 | Senior | Incremental processing, metrics, excellent reflection |
| 80-89 | Mid-Senior | All critical fixes + good tests + monitoring awareness |
| 70-79 | Mid | Idempotent, secure, validated, tested |
| 60-69 | Junior-Mid | Basic fixes, some tests |
| <60 | Fail | Misses critical issues |

---

## 🔴 Critical Issues (Must Fix)

1. **Analytics table appends on every run** → Use TRUNCATE or UPSERT
2. **Hardcoded credentials** → Use Airflow Connections
3. **SQL injection** → Use parameterized queries
4. **No raw schema** → Create raw layer
5. **DROP TABLE pattern** → Use transactions or temp tables

---

## 🚩 AI Detection Red Flags

- Perfect code but shallow reflection
- Cannot explain decisions in interview
- Over-engineering without justification
- Inconsistent code quality

---

## 📁 Key Documents

**For Candidates:**
- `README.md` - Setup guide
- `CHALLENGE_INSTRUCTIONS.md` - Official challenge

**For Evaluators (DO NOT SHARE):**
- `ANSWER_KEY.md` - All flaws + solutions
- `implementation_plan.md` - Architecture details

---

## 🎯 Next Steps

1. ✅ Test the pipeline yourself
2. ✅ Verify idempotency bug
3. ✅ Review answer key
4. ✅ Create GitHub template
5. ✅ Distribute to candidates
