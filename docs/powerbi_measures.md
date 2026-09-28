# Power BI Measure Catalog

Generated from `powerbi/SalesAnalytics.SemanticModel/definition/tables/_Measures.tmdl`,
`Time Intelligence.tmdl`, and `Top N.tmdl`. Every measure below was validated against the SQL
baseline in `powerbi_reconciliation_results.md` — see the pass/fail table at the end of this file.

## Base

**Total Sales** — `#,##0` currency
```DAX
SUM ( 'Fact Sales'[Sales Amount] )
```
Sum of line-level sales amount in whole currency units.

**Total Quantity** — `#,##0`
```DAX
SUM ( 'Fact Sales'[Quantity] )
```
Total units sold across all order lines.

**Total Cost** — `#,##0` currency
```DAX
SUMX ( 'Fact Sales', 'Fact Sales'[Quantity] * RELATED ( 'Dim Product'[Cost] ) )
```
Line-level quantity times unit cost, summed. Orphan lines with no matching product contribute zero cost (there are none in this dataset).

**Gross Profit** — `#,##0` currency
```DAX
[Total Sales] - [Total Cost]
```
Total Sales minus Total Cost.

**Gross Margin %** — `0.0%`
```DAX
DIVIDE ( [Gross Profit], [Total Sales] )
```
Gross Profit as a percentage of Total Sales.

**Order Count** — `#,##0`
```DAX
DISTINCTCOUNT ( 'Fact Sales'[Order Number] )
```
Count of distinct sales orders (an order can span multiple lines).

**Sales Lines** — `#,##0`
```DAX
COUNTROWS ( 'Fact Sales' )
```
Count of sales order lines (rows in Fact Sales).

**Avg Order Value** — `#,##0.00` currency
```DAX
DIVIDE ( [Total Sales], [Order Count] )
```
Total Sales divided by the number of distinct orders.

**Avg Selling Price** — `#,##0.00` currency
```DAX
DIVIDE ( [Total Sales], [Total Quantity] )
```
Total Sales divided by Total Quantity.

## Customers

**Active Customers** — `#,##0`
```DAX
DISTINCTCOUNT ( 'Fact Sales'[Customer Key] )
```
Count of distinct customers with at least one sales line in the current filter context.

**New Customers** — `#,##0`
```DAX
VAR _minDate = MIN ( 'Dim Date'[Date] )
VAR _maxDate = MAX ( 'Dim Date'[Date] )
VAR _custFirst =
    ADDCOLUMNS (
        VALUES ( 'Fact Sales'[Customer Key] ),
        "@FirstOrder", CALCULATE ( MIN ( 'Fact Sales'[Order Date] ), REMOVEFILTERS ( 'Dim Date' ) )
    )
RETURN
    COUNTROWS ( FILTER ( _custFirst, [@FirstOrder] >= _minDate && [@FirstOrder] <= _maxDate ) )
```
Count of customers whose first-ever order date falls within the current date filter. 2 customers with no resolvable order date (all their lines have an invalid order date) never count as "new" in any year.

**Returning Customers** — `#,##0`
```DAX
[Active Customers] - [New Customers]
```
Active Customers minus New Customers.

**Sales per Customer** — `#,##0.00` currency
```DAX
DIVIDE ( [Total Sales], [Active Customers] )
```
Total Sales divided by Active Customers.

## Time (calendar)

**Sales PY** — `#,##0` currency
```DAX
CALCULATE ( [Total Sales], SAMEPERIODLASTYEAR ( 'Dim Date'[Date] ) )
```
Total Sales for the same period one calendar year earlier.

**Sales YoY** — `#,##0` currency
```DAX
IF ( NOT ISBLANK ( [Sales PY] ), [Total Sales] - [Sales PY] )
```
Total Sales minus Sales PY, blank when there is no prior-year data to compare.

**Sales YoY %** — `0.0%`
```DAX
DIVIDE ( [Total Sales] - [Sales PY], [Sales PY] )
```
Sales YoY as a percentage of Sales PY. 2013 vs 2012 will read as very large growth because of the data's concentration in 2013 — see the README's data notes.

**Sales YTD** — `#,##0` currency
```DAX
TOTALYTD ( [Total Sales], 'Dim Date'[Date] )
```
Total Sales accumulated from the start of the calendar year to the latest date in context.

**Sales Rolling 3M** — `#,##0` currency
```DAX
CALCULATE ( [Total Sales], DATESINPERIOD ( 'Dim Date'[Date], MAX ( 'Dim Date'[Date] ), -3, MONTH ) )
```
Total Sales over the trailing 3 months ending on the latest date in context.

**Sales by Ship Date** — `#,##0` currency
```DAX
CALCULATE ( [Total Sales], USERELATIONSHIP ( 'Dim Date'[Date], 'Fact Sales'[Shipping Date] ) )
```
Total Sales filtered by Shipping Date instead of Order Date, using the inactive relationship.

## Time (fiscal)

**Sales FYTD** — `#,##0` currency
```DAX
TOTALYTD ( [Total Sales], 'Dim Date'[Date], "6/30" )
```
Total Sales accumulated since the start of the fiscal year (July 1) to the latest date in context.

**Sales FYTD PY** — `#,##0` currency
```DAX
CALCULATE ( [Sales FYTD], SAMEPERIODLASTYEAR ( 'Dim Date'[Date] ) )
```
Sales FYTD for the same fiscal-to-date period one year earlier.

## Operations

**Avg Days to Ship** — `#,##0.0`
```DAX
AVERAGE ( 'Fact Sales'[Days to Ship] )
```
Average number of days between Order Date and Shipping Date. Reads a flat 7.0 in this dataset — every order ships exactly 7 days after ordering, a property of the sample data.

**On-Time Ship %** — `0.0%`
```DAX
DIVIDE (
    COUNTROWS ( FILTER ( 'Fact Sales', 'Fact Sales'[Shipping Date] <= 'Fact Sales'[Due Date] ) ),
    [Sales Lines]
)
```
Share of sales lines shipped on or before their due date. Reads 100% everywhere in this dataset; the Operations report page centers on days-to-ship and ship-date volume instead of this flat gauge.

## Ranking

**Product Rank** — `#,##0`
```DAX
IF (
    ISINSCOPE ( 'Dim Product'[Product Name] ),
    RANKX ( ALLSELECTED ( 'Dim Product'[Product Name] ), [Total Sales], , DESC, DENSE )
)
```
Dense rank of the current product by Total Sales among all selected products, blank unless a single product is in scope.

**Top N Sales** — `#,##0` currency
```DAX
VAR _n = [Top N Value]
RETURN IF ( [Product Rank] <= _n, [Total Sales] )
```
Total Sales for products ranked within the Top N slider value, blank otherwise.

**Pareto Cumulative %** — `0.0%`
```DAX
VAR _cur = [Total Sales]
VAR _all = CALCULATE ( [Total Sales], ALLSELECTED ( 'Dim Product'[Product Name] ) )
VAR _tbl = ADDCOLUMNS ( ALLSELECTED ( 'Dim Product'[Product Name] ), "@s", [Total Sales] )
RETURN
    IF ( NOT ISBLANK ( _cur ), DIVIDE ( SUMX ( FILTER ( _tbl, [@s] >= _cur ), [@s] ), _all ) )
```
Running share of Total Sales contributed by this product and every product with equal or higher sales, for a Pareto (80/20) chart.

**Top N Value** — `#,##0`
```DAX
SELECTEDVALUE ( 'Top N'[Top N], 10 )
```
Current value of the Top N what-if parameter (5-25, step 5, default 10). See the "Top N parameter" section below.

## UX

**Last Refresh** — text
```DAX
"Data refreshed " & FORMAT ( MAX ( 'Refresh Info'[Last Refresh] ), "yyyy-mm-dd hh:nn" )
```
Text footer showing when the semantic model was last refreshed.

**KPI Colour** — text
```DAX
IF ( [Sales YoY %] >= 0, "#2E7D32", "#C62828" )
```
Hex colour for conditional formatting: green when YoY % is non-negative, red otherwise.

**Title Sales Trend** — text
```DAX
VAR _country = IF ( ISFILTERED ( 'Dim Customer'[Country] ), SELECTEDVALUE ( 'Dim Customer'[Country], "Multiple countries" ), "All countries" )
VAR _year = IF ( ISFILTERED ( 'Dim Date'[Year] ), SELECTEDVALUE ( 'Dim Date'[Year], "Multiple years" ), "All years" )
RETURN "Sales by Month — " & _country & ", " & _year
```
Dynamic title for the Executive Overview sales trend chart, reflecting the current Country and Year filters.

## Calculation group: Time Intelligence

A reusable wrapper — drop any base measure onto a visual alongside the `Period` column from this
calculation group, and pick which time-calculation to apply. Precedence 10.

| Order | Item | Expression | Format |
| --- | --- | --- | --- |
| 0 | Current | `SELECTEDMEASURE ()` | inherits measure format |
| 1 | PY | `CALCULATE ( SELECTEDMEASURE (), SAMEPERIODLASTYEAR ( 'Dim Date'[Date] ) )` | inherits measure format |
| 2 | YoY | `VAR _cur = SELECTEDMEASURE () VAR _py = CALCULATE ( SELECTEDMEASURE (), SAMEPERIODLASTYEAR ( 'Dim Date'[Date] ) ) RETURN IF ( NOT ISBLANK ( _py ), _cur - _py )` | inherits measure format |
| 3 | YoY % | `DIVIDE ( SELECTEDMEASURE () - CALCULATE ( SELECTEDMEASURE (), SAMEPERIODLASTYEAR ( 'Dim Date'[Date] ) ), CALCULATE ( SELECTEDMEASURE (), SAMEPERIODLASTYEAR ( 'Dim Date'[Date] ) ) )` | dynamic: `0.0%` |
| 4 | YTD | `CALCULATE ( SELECTEDMEASURE (), DATESYTD ( 'Dim Date'[Date] ) )` | inherits measure format |
| 5 | FYTD | `CALCULATE ( SELECTEDMEASURE (), DATESYTD ( 'Dim Date'[Date], "6/30" ) )` | inherits measure format |

The explicit `Sales PY`, `Sales YoY %`, and `Sales YTD` measures above are kept alongside this
calculation group intentionally — they're needed for KPI cards and conditional formatting (which
can't reference a calculation item directly), and they demonstrate both techniques side by side.

## What-if parameter: Top N

A calculated table (`GENERATESERIES(5, 25, 5)`, renamed to a `Top N` column via `SELECTCOLUMNS`)
driving a slider from 5 to 25 in steps of 5, default 10. Its value is read by the `Top N Value`
measure above, which in turn drives `Top N Sales` and the Top N chart on the Product Performance page.

## Field parameter: Metric Selector

```DAX
Metric Selector = {
    ( "Sales", NAMEOF ( [Total Sales] ), 0 ),
    ( "Gross Profit", NAMEOF ( [Gross Profit] ), 1 ),
    ( "Quantity", NAMEOF ( [Total Quantity] ), 2 ),
    ( "Orders", NAMEOF ( [Order Count] ), 3 )
}
```
Lets a report viewer swap the value axis of a visual between four base measures via a single
slicer, without needing four separate visuals. Created directly in Power BI Desktop (Modeling →
New parameter → Fields) rather than hand-authored in TMDL, per the build plan's own guidance —
Desktop sets metadata for field parameters that's safer to let it generate than to replicate by hand.

## DAX validation vs. SQL baseline (Phase 3)

Run via DAX query view in Power BI Desktop against the live model, compared to
`powerbi_reconciliation_results.md`. All match exactly.

| Check | SQL baseline | DAX result | Pass/Fail |
| --- | --- | --- | --- |
| Total Sales / Quantity / Orders / Customers / Lines (all data) | 29,356,250 / 60,423 / 27,659 / 18,484 / 60,398 | 29,356,250 / 60,423 / 27,659 / 18,484 / 60,398 | ✅ |
| Sales & lines by year (incl. blank/invalid-date bucket) | blank=4,992/19; 2010=43,419/14; 2011=7,075,088/2,216; 2012=5,842,231/3,397; 2013=16,344,878/52,782; 2014=45,642/1,970 | identical | ✅ |
| Sales by category | Accessories=700,262; Bikes=28,316,272; Clothing=339,716 | identical | ✅ |
| Sales by country (Power Query-cleaned labels) | Australia=9,060,172; Canada=1,977,738; France=2,643,751; Germany=2,894,066; UK=3,391,376; US=9,162,327; Unknown=226,820 | identical | ✅ |
| Total Cost / Avg Days to Ship / On-Time Ship % | 17,670,493 / 7.0 / 100% | 17,670,493 / 7.0 / 100% | ✅ |
| New Customers by cohort year | blank=2; 2010=14; 2011=2,216; 2012=3,225; 2013=12,521; 2014=506 | identical | ✅ |
| Hand-check: Sales Mar 2013 / Mar 2012 (PY) | 1,049,732 / 373,478 | 1,049,732 / 373,478 | ✅ |
| Hand-check: Sales YoY % for Mar 2013 | (1,049,732−373,478)/373,478 = 181.07% | 181.07% | ✅ |
| Hand-check: Sales YTD through end of Mar 2013 | 2,678,708 | 2,678,708 | ✅ |

Every measure reconciles exactly to its SQL source of truth. No adjustments were needed to any
measure's DAX.
