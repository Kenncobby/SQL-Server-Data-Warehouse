# Power BI QA Checklist

Tested in Power BI Desktop against the final Phase 7/8 report. Interactive elements
(page-navigation buttons, the "Reset Filters" bookmark button) only fire on a plain
click in **Reading view** or once published — in **Edit view** they require
**Ctrl+click**, which is expected Desktop behavior, not a defect.

| # | Check | Result | Notes |
|---|---|---|---|
| 1 | All reconciliation checks pass | ✅ Pass | See `powerbi_measures.md` pass/fail table — every measure ties to the Phase 3 SQL baseline. |
| 2 | Slicers sync correctly | ✅ Pass | Selecting a slicer value on Executive Overview carries through to Product Performance, Customer Insights, and Operations via sync groups. |
| 3 | Drill-through works from the Product matrix | ✅ Pass | Requires expanding **Category → Subcategory → Product Name** first — drill-through is keyed to the `Product Name` field, so it only appears once a leaf-level product row is right-clicked. |
| 4 | Drill-through works from the Top N chart | ✅ Pass | Confirmed directly on "Top N Products by Sales" (Product Performance). |
| 5 | The tooltip page appears on hover | ✅ Pass | Category Tip is wired to "Sales by Category" on Executive Overview; hovering a bar shows the tooltip page. ("Top N Products by Sales" shows a drill-through prompt instead, by design — it has its own drill-through target, not a tooltip.) |
| 6 | Bookmarks reset filters | ✅ Pass | "Reset Filters" button clears all slicer selections when clicked (Ctrl+click in Edit view). |
| 7 | Navigation buttons work | ✅ Pass | All page-navigation buttons jump to the correct page (Ctrl+click in Edit view). |
| 8 | No visual shows an error or "(Blank)" category without an explanation | ✅ Pass | Two known, documented placeholders appear by design: `Country` = "Unknown" for cleaned `n/a` source rows, `Age Band` = "Unknown" for null birthdates (see `Dim Customer.tmdl` comments); "(Blank)" rows on date-based visuals trace to the 19 invalid order dates flagged in `powerbi_reconciliation_results.md`. Both are called out in the README Data Notes. |
| 9 | The mobile layout renders | ✅ Pass | Executive Overview renders correctly in Desktop's mobile/phone preview. |
| 10 | RLS roles filter correctly | ✅ Pass | Viewing as North America drops Total Sales from $29.4M to $11M — see `docs/images/pbi_rls_view_as.png`. |

**Result: 10/10 pass.**
