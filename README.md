# 📊 ABXplore: A/B Testing Analytics Platform

## 🧠 Project Summary
This project simulates a **real-world A/B testing environment** to evaluate the impact of a new lead onboarding process launched in June 2024. The goal is to demonstrate the complete **analytical journey** — from identifying the business problem, through exploratory analysis, hypothesis formation, and statistical testing — all the way to actionable recommendations.

The platform uses **Python, dbt, SQL, and Streamlit** to simulate CRM workflows and transform raw data into actionable insights.

---

## 📌 Business Problem
> "Our lead conversion rates have declined in early 2024. Can a new onboarding process fix this? If yes, how much improvement does it bring, and for which segments?"

---

## 🎯 Business Objective

Determine whether a newly designed lead onboarding strategy improves:

- ✅ Conversion rates (Closed Won)
- ✅ Time to close
- ✅ Lead responsiveness
- ✅ Revenue per lead

This analysis helps decide whether to roll out the new onboarding strategy across all users.

---

## 🧩 Project Workflow

```
1. Simulate Raw CRM Data (Python)
   ↓
2. Clean & QA (SQL / Python)
   ↓
3. Build dbt Models (stg → int → mart)
   ↓
4. Calculate A/B Test Metrics (SQL)
   ↓
5. Run Statistical Tests (Python: t-test, chi-square)
   ↓
6. Build Interactive Dashboard (Tableau)
   ↓
7. Add Final Recommendation (Based on metrics & p-values)
```
