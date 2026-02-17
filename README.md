# Sysco Case Study — Pricing Analytics Engine
**Data Source:** Sysco Arkansas Price Sheet (Statewide Groceries)  
**Analysis Period:** Oct 6, 2025 — Jan 25, 2026 (16 weeks)  
**Author:** Ayush Bhardwaj

---

## Architecture

```
data_ingestion.py      →  Parses real Sysco pricing data, generates realistic
                           customer base (77 accounts, 5 segments) and 82K+
                           transaction records with cost shocks and overrides

analytics_engine.py    →  Core pricing intelligence:
                           Module 1: Weekly Portfolio Summary (operating rhythm)
                           Module 2: Margin Bridge (Price/Cost/Volume/Mix decomposition)
                           Module 3: Override Recommendation Engine (955 actions, $147K annual GP)
                           Module 4: Lever Change Impact Analysis (commodity cost shock)
                           Module 5: Scenario Modeling (3 pass-through strategies)
                           Module 6: Data Integrity & QA Checks
                           Module 7: Basket Analysis

sysco_pricing_dashboard.jsx  →  Interactive React dashboard for presentation
```

## How to Run

```bash
pip install pandas numpy scipy
python data_ingestion.py          # generates product catalog + transactions
python analytics_engine.py        # runs all 7 analytics modules + exports JSON
```

## Key Outputs

| Metric | Value |
|--------|-------|
| Total Net Sales (16 wk) | $15.4M |
| Average GP% | 23.0% |
| Override Recommendations | 955 (below 18% GP floor) |
| Projected Annual GP Recovery | $147,487 |
| Recommended Strategy | Scenario B: Targeted Overrides by Segment |

## Tech Stack

- **Python** (pandas, numpy, scipy) — data pipeline + statistical modeling
- **SQL-equivalent** — CTEs, window functions via pandas groupby
- **React & Recharts** — interactive dashboard
- All analysis is reproducible from the raw Sysco price sheet
