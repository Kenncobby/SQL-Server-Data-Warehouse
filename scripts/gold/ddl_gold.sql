/*
====================================================================
DDL Script: Create Gold Layer (Views)
====================================================================
Script Purpose:
		This script creates views for the Gold layer of the 
		Data Warehouse. The Gold layer has the final dimension 
		and fact tables (Star Schema)

		Each view perfoms transformations and combines data 
		from the silver layer to produce a clean, enriched 
		and business-ready dataset

Usage:
	- These views can be queried directly for analytics and reporting
======================================================================
*/


--====================================================================
-- Create Dimension: gold.dim_customers
--====================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
	DROP VIEW gold.dim_customers;
GO

CREATE OR ALTER VIEW gold.dim_customers AS
Select
	ROW_NUMBER() OVER(ORDER BY cst_id) as customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.CNTRY as country,
	ci.cst_marital_status AS marital_status,
	case when ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- Use CRM as Master data for Gender info
		ELSE COALESCE(ca.gen, 'n/a')   -- fall back to ERP data
	END AS gender,
	ca.BDATE as birthdate,
	ci.cst_create_date AS create_date
from silver.crm_cust_info as ci
Left Join silver.erp_cust_az12 as ca
ON CI.cst_key=CA.CID
left join silver.erp_loc_a101 as la
ON ci.cst_key=la.cid
GO

--====================================================================
-- Create Dimension: gold.dim_products
--====================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
	DROP VIEW gold.dim_products;
GO

CREATE OR ALTER VIEW gold.dim_products as
Select 
	ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) as product_key,
	pn.prd_id as product_id,
	pn.prd_key as product_number,
	pn.prd_nm as product_name,
	pn.cat_id as category_id,
	pc.CAT as category,
	pc.SUBCAT as subcategory,
	pc.MAINTENANCE as maintenance,
	pn.prd_cost as cost,
	pn.prd_line as product_line,
	pn.prd_start_dt as start_date
from silver.crm_prd_info as pn
Left join silver.erp_px_cat_g1v2 as pc
ON pn.cat_id=pc.ID
WHERE prd_end_dt is null -- Filter out all historical data
GO

--====================================================================
-- Create Fact Table: gold.fact_sales
--====================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
	DROP VIEW gold.fact_sales;
GO

CREATE OR ALTER VIEW gold.fact_sales AS
SELECT 
	  sd.sls_ord_num as order_number,
	  pr.product_key,
	  cu.customer_key,
      sd.sls_order_dt as order_date,
      sd.sls_ship_dt as shipping_date,
      sd.sls_due_dt as due_date,
      sd.sls_sales as sales_amount,
      sd.sls_quantity as quantity,
      sd.sls_price as price
FROM silver.crm_sales_details sd
left Join  gold.dim_products pr
ON sd.sls_prd_key=pr.product_number
left join gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id
GO
