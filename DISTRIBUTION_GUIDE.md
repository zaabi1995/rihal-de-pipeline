# 📦 File Distribution Guide

## For Candidates (GitHub Repository)

Include these files in the candidate repository:

### ✅ Core Files
- `docker-compose.yml`
- `Dockerfile`
- `.gitignore`
- `README.md`
- `CHALLENGE_INSTRUCTIONS.md`

### ✅ Code & Scripts
- `dags/shipment_analytics_dag.py`
- `scripts/extract_shipments.py`
- `scripts/extract_customer_tiers.py`
- `scripts/transform_data.py`
- `scripts/load_analytics.py`

### ✅ Data & Config
- `data/customer_tiers.csv`
- `sql/init.sql`
- `api/` (entire folder)

### ✅ Templates & Tests
- `tests/test_sample.py`
- `tests/requirements.txt`
- `ENGINEERING_AUDIT.template.md` (optional)
- `DESIGN_REFLECTION.template.md` (optional)

---

## For Evaluators Only (Private Documents)

**⚠️ NEVER share these with candidates:**

### 🔒 Answer Keys & Grading
- `ANSWER_KEY.md` (in brain folder)
- `implementation_plan.md` (in brain folder)
- `walkthrough.md` (in brain folder)
- `QUICK_START_EVALUATORS.md`

These contain:
- All 22+ intentional flaws documented
- Expected solutions by skill level
- Grading rubric
- Interview questions
- Red flags for AI submissions

---

## Quick Checklist Before Distribution

Before sending to candidates:

- [ ] Remove or move `QUICK_START_EVALUATORS.md` 
- [ ] Remove brain folder artifacts
- [ ] Verify no "Flaw:" comments in code
- [ ] Test that `docker-compose up` works
- [ ] Verify pipeline runs (but has issues)
- [ ] README is clear and helpful
- [ ] No answer keys in repository

---

## Distribution Methods

### Option 1: GitHub Template Repository
1. Create private template repository
2. Include only candidate files (see list above)
3. Candidates fork/clone for their submission

### Option 2: ZIP Distribution
1. Create ZIP with candidate files only
2. Email to candidates with deadline
3. Request private GitHub submission

---

**Remember: The challenge works because candidates discover issues themselves, not because we tell them what's wrong!**
