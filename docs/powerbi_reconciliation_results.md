# Power BI Reconciliation Baseline

These are the SQL-source-of-truth figures every Power BI measure in `SalesAnalytics.pbip` must
match exactly (see `powerbi_measures.md` for the Phase 6 pass/fail comparison). Produced by
`tests/powerbi_reconciliation.sql` against `gold.*` on 2026-09-26, after the Phase 2 warehouse
load.

## Caveats that explain small gaps

- **19 sales lines have an invalid/blank `order_date`** (nulled out in Silver). They're included
  in every "all data" total below, but fall into a separate `NULL` row in the by-year breakdown.
- **2 customers have *every* order line with a null `order_date`.** They're excluded entirely from
  the "New Customers" cohort baseline (table 8), since a first-order year can't be determined for
  them. This is why the cohort counts sum to 18,482, not the full 18,484 distinct customers.
- **Country values are raw** (`GERMANY` in all caps, `n/a` for unresolved). Power BI will display
  these as `Germany` and `Unknown` (Phase 4 Power Query cleanup) — the totals below are the ones
  those relabeled categories must match, unchanged.
- **Zero orphan lines**: every `fact_sales` row has a valid `customer_key` and `product_key` (see
  Phase 2 checkpoint), so no "Unknown member" adjustment is needed anywhere below.

## 1. Overall totals — all data

| Total Sales | Total Quantity | Distinct Orders | Distinct Customers (with sales) | Line Count |
|---:|---:|---:|---:|---:|
| 29,356,250 | 60,423 | 27,659 | 18,484 | 60,398 |

## 2. Same metrics, by calendar year

| Order Year | Total Sales | Total Quantity | Distinct Orders | Distinct Customers | Line Count |
|---|---:|---:|---:|---:|---:|
| *(NULL — invalid date)* | 4,992 | 19 | 15 | 15 | 19 |
| 2010 | 43,419 | 14 | 14 | 14 | 14 |
| 2011 | 7,075,088 | 2,216 | 2,216 | 2,216 | 2,216 |
| 2012 | 5,842,231 | 3,397 | 3,269 | 3,255 | 3,397 |
| 2013 | 16,344,878 | 52,807 | 21,287 | 17,427 | 52,782 |
| 2014 | 45,642 | 1,970 | 871 | 834 | 1,970 |

2013 dominates the dataset (87% of lines), so any 2013-vs-2012 YoY comparison shows outsized
growth. This is called out in the README's data notes rather than hidden.

## 3. Total sales by category

| Category | Total Sales |
|---|---:|
| Bikes | 28,316,272 |
| Accessories | 700,262 |
| Clothing | 339,716 |

## 4. Total sales by country

| Country (raw) | Total Sales |
|---|---:|
| United States | 9,162,327 |
| Australia | 9,060,172 |
| United Kingdom | 3,391,376 |
| GERMANY | 2,894,066 |
| France | 2,643,751 |
| Canada | 1,977,738 |
| n/a | 226,820 |

## 5. Total cost

`SUM(quantity * cost)` joined to `dim_products` (orphan lines, of which there are none, would
contribute 0 cost):

| Total Cost |
|---:|
| 17,670,493 |

Implied Gross Profit = 29,356,250 − 17,670,493 = **11,685,757**. Gross Margin % = **39.8%**.

## 6. Average days to ship

| Avg Days to Ship |
|---:|
| 7.000000 |

Every order in this dataset ships exactly 7 days after the order date — a known property of this
sample data, not a bug.

## 7. On-time ship share

| Share shipped on/before due date |
|---:|
| 100% |

Confirms the build prompt's expectation: `On-Time Ship %` will read 100% everywhere. The
Operations report page centers on days-to-ship and order volume instead of a flat gauge.

## 8. New Customers baseline (first-order cohort year)

| Cohort Year | New Customers |
|---|---:|
| 2010 | 14 |
| 2011 | 2,216 |
| 2012 | 3,225 |
| 2013 | 12,521 |
| 2014 | 506 |
| **Total** | **18,482** (of 18,484 — 2 customers have no resolvable first-order date) |
