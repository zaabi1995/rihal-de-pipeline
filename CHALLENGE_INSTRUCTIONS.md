# 🏗 The 2026 Rihal Data Engineering Talent Challenge
## "The Broken Pipeline"

## 1. Overview

Welcome to the Rihal Data Engineering Talent Challenge.

You have inherited a partially implemented data pipeline that powers internal reporting.

**The system is functional — but not production-ready.**

Your task is not to build from scratch.

Your task is to:

✅ **Audit. Improve. Harden. Justify.**

This challenge reflects real engineering work at Rihal:
*You rarely start with perfect systems — you improve them.*

---

## 2. Duration

**2 Weeks**

---

## 3. Required Stack

You must use the provided technologies:

- Apache Airflow
- Docker & docker-compose
- Python 3.9+
- PostgreSQL
- SQL

You may restructure or refactor as needed, but these core technologies must remain.

---

## 4. The Scenario

Rihal ingests shipment data from:

1. A REST API (external system)
2. A legacy CSV file (customer tier information)

This data feeds internal analytics, including:

**Total Shipping Spend per Customer Tier per Month**

⚠️ The current pipeline runs — but you should not assume it is correct, reliable, or scalable.

---

## 5. Your Responsibilities

You are acting as the engineer responsible for making this system production-ready.

### Task 1 — Engineering Audit

Create a document:
```
ENGINEERING_AUDIT.md
```

In this document:

- Identify critical weaknesses in the current system
- Rank them by severity (High / Medium / Low)
- Explain the potential production impact of each issue
- Describe how you mitigated or redesigned them

**There is no fixed number of issues.**

We evaluate clarity of reasoning and prioritization.

---

### Task 2 — Improve the Pipeline

Refactor and improve the pipeline so that it is production-ready.

Your production-ready system should demonstrate engineering best practices including:

- Data Integrity & Consistency
- Fault Tolerance & Resilience
- Security & Compliance
- Maintainability & Observability
- Scalability Considerations

The specific implementation approach and prioritization is up to you.

---

### Task 3 — Data Integrity & Modeling

Ensure the system produces correct analytical output:

**Total Shipping Spend per Customer Tier per Month**

Your solution should demonstrate:

- Logical data modeling
- Correct handling of conflicting or duplicate records
- Clear transformation boundaries
- Consistent aggregation logic

**If you encounter ambiguous scenarios, document your assumptions and justify your decisions.**

Engineering judgment is part of the evaluation.

---

### Task 4 — Testing (Mandatory)

Create a `tests/` folder.

Your tests must validate **meaningful business logic**.

At minimum, your test suite should demonstrate that:

- Core transformation logic behaves as expected
- Rerunning the pipeline does not corrupt results
- Critical edge cases are handled correctly

⚠️ Superficial or trivial tests will not receive credit.

---

### Task 5 — Design Reflection

Create:
```
DESIGN_REFLECTION.md
```

Answer:

1. What was the most critical issue you discovered?
2. What trade-offs did you make?
3. Where could your solution still fail?
4. How would this system behave at 100x data volume?
5. What would you redesign with more time?

Additionally:

- Explain one complex function line-by-line
- Explain one SQL transformation step-by-step
- Describe one alternative design you considered and rejected

**Clarity and depth of reasoning are heavily weighted.**

---

## 6. Constraints

Your submission must:

- ✅ Run via `docker-compose up`
- ✅ Not require manual database intervention
- ✅ Not contain hardcoded credentials
- ✅ Include clear setup instructions in README.md
- ✅ Include meaningful tests
- ✅ Include both required documentation files

⚠️ Submissions missing documentation or tests will not be reviewed.

---

## 7. Evaluation Criteria

We evaluate across the following dimensions:

### 1️⃣ Engineering Judgment
- Can you identify real risks?
- Do you prioritize correctly?

### 2️⃣ Data Integrity Thinking
- Does your pipeline preserve correctness?
- Is it safe to rerun?
- Are transformations logically sound?

### 3️⃣ Code Quality
- Structure and modularity
- Readability
- Logging
- Error handling
- Maintainability

### 4️⃣ Testing Depth
- Do tests validate business logic?
- Do they catch meaningful failures?

### 5️⃣ Systems Thinking
- Awareness of failure modes
- Scalability considerations
- Clear separation of concerns

### 6️⃣ Reflection Quality
- Clear reasoning
- Honest assessment of trade-offs
- Technical maturity

---

## 8. Submission Process

Submit a private GitHub repository containing:

```
docker-compose.yml
dags/
scripts/
sql/
tests/
ENGINEERING_AUDIT.md
DESIGN_REFLECTION.md
README.md
```

After review:

- Top submissions will be shortlisted
- Final winners will be invited to interview with Rihal

---

## 9. Important Notes

✅ We do not expect perfection.

✅ We do expect structured thinking.

✅ There is no single "correct" architecture.

✅ Ambiguity is intentional.

**Strong submissions demonstrate:**

- Ownership
- Curiosity
- Critical thinking
- Engineering discipline

---

**Good luck! We're excited to see your approach.**

*— The Rihal Data Engineering Team*
