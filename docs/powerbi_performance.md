# Power BI Performance Analyzer Results

Captured in Power BI Desktop via **View → Performance analyzer**, one recording session
covering all 6 report pages (page navigation via `Change page`, visuals refreshed on
each page before moving to the next). Raw export: `PowerBIPerformanceData.json`
(Performance Analyzer → Export).

**Target**: every visual under 1,000 ms. Result: **pass** — slowest visual on the
report is 728 ms.

DAX Studio and Tabular Editor 2 were not available in this environment, so Server
Timings and Best Practice Analyzer passes were skipped; Performance Analyzer numbers
below are the only performance evidence for this project.

## Executive Overview

| Visual | Type | Duration |
|---|---|---:|
| Card (Last Refresh) | card | 412 ms |
| Order Count | card | 411 ms |
| Gross Margin % | card | 405 ms |
| Gross Profit | card | 400 ms |
| Sales by Month vs Prior Year | line chart | 345 ms |
| Total Sales | card | 342 ms |
| Total Sales by Country | Azure map | 289 ms |
| Sales by Category | bar chart | 199 ms |
| Slicers / buttons / text box | — | 123–151 ms |

## Product Performance

| Visual | Type | Duration |
|---|---|---:|
| Product Hierarchy Detail | table | 728 ms |
| Top N Products by Sales | bar chart | 619 ms |
| Pareto: Sales Concentration | line/column combo | 613 ms |
| Quantity vs Margin by Subcategory | scatter chart | 518 ms |
| Category / Subcategory / Top N slicers | slicer | 266–477 ms |
| Buttons / text box | — | 109–138 ms |

## Customer Insights

| Visual | Type | Duration |
|---|---|---:|
| Returning Customers | card | 685 ms |
| New Customers | card | 681 ms |
| Sales per Customer | card | 673 ms |
| Active Customers | card | 646 ms |
| Top Customers by Sales | table | 630 ms |
| New vs Returning Customers by Month | column chart | 629 ms |
| Sales by Gender | donut chart | 627 ms |
| Total Sales by Age Band | column chart | 623 ms |
| Slicers / buttons / text box | — | 107–268 ms |

## Operations

| Visual | Type | Duration |
|---|---|---:|
| Sales, Gross Profit, Quantity and Orders by Year-Month | column chart | 676 ms |
| Sales Lines | card | 614 ms |
| On-Time Ship % | card | 612 ms |
| Metric Selector slicer | slicer | 585 ms |
| Avg Days to Ship | card | 580 ms |
| Sales by Order Date vs Ship Date | column chart | 580 ms |
| Other slicers / buttons / text box | — | 124–276 ms |

## Product Detail (drill-through)

| Visual | Type | Duration |
|---|---|---:|
| Quantity | card | 512 ms |
| Rank | card | 506 ms |
| Margin | card | 500 ms |
| Customers Who Bought This Product | table | 498 ms |
| Profit | card | 492 ms |
| Sales | card | 469 ms |
| Sales Trend | line chart | 452 ms |
| Button / text box | — | 72–92 ms |

## Category Tip (tooltip)

| Visual | Type | Duration |
|---|---|---:|
| Gross Margin | card | 300 ms |
| Sales Trend | line chart | 232 ms |

## Summary

- 61 visuals timed across 6 pages; **none exceeded the 1,000 ms target**.
- Slowest visual overall: **Product Hierarchy Detail** (table, Product Performance, 728 ms) — expected, as it's the only matrix-style visual with the deepest row count (Category → Subcategory → Product).
- Card visuals cluster around 400–700 ms on first load of a page (cold visual cache); this is normal Desktop behavior and not a modeling issue.
- No DAX-level optimization was needed as a result of this pass.
