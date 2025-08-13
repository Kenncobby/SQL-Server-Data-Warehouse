/*
=============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
=============================================================================
Script Purpose: 
    This stored procedure performs the ETL (Extract, Transform, Load) process 
    to populate the 'silver' schema tables from the 'bronze' schema.
Actions Performed:
    - Truncates Silver tables
    - Inserts transformed and cleaned data from Bronze to Silver tables.

Parameters:
    None.
    This Stored procedure does not accep any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=========================================';
		PRINT 'Silver Layer Loaded';
		PRINT '=========================================';

		PRINT '-----------------------------------------';
		PRINT 'CRM Tables Loaded';
		PRINT '-----------------------------------------';

		-- Loading silver.crm_cust_info
		SET @start_time = GETDATE();
		PRINT '>> Truncated Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserted Data Into: silver.crm_cust.info';
		with Flagging as(
		Select * ,
		Row_number() over (partition by cst_id order by cst_create_date desc) as flag_last
		from bronze.crm_cust_info
		where cst_id is not null
		)

		Insert into silver.crm_cust_info ( cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
		Select 
			cst_id,
			cst_key,
			TRIM(cst_firstname) as cst_firstname,
			TRIM(cst_lastname) as cst_lastname,
			case
				when upper(TRIM(cst_marital_status))='M' then 'Married'
				when upper(TRIM(cst_marital_status))='S' then 'Single'
				Else 'n/a' End as cst_marital_status, -- Normalize marital status values to readable format
			case
				when upper(TRIM(cst_gndr))='M' then 'Male'
				when upper(TRIM(cst_gndr))='F' then 'Female'
				Else 'n/a' End as cst_gndr, -- Normalize gender values to readable format
			cst_create_date
		from flagging
		where flag_last = 1;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '-----------------------';

		-- Loading silver.crm_prd_info
		SET @start_time = GETDATE();
		PRINT '>> Truncated Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserted Data Into: silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info (prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
		 select 
			prd_id,	
			Replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
			SUBSTRING(prd_key,7, len(prd_key)) as prd_key,
			prd_nm,	
			ISNULL(prd_cost, 0) as prd_cost,
			case upper(TRIM(prd_line))
				when 'M' then 'Mountain'
				when 'R' then 'Road'
				when 'S' then 'Other Sales'
				when 'T' then 'Touring'
				Else 'n/a' 
			End as prd_line, -- Map product line code to descriptive values
			CAST (prd_start_dt AS DATE), 
			CAST (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) 
				AS prd_end_dt -- Calculate end date as one day befor the next start date
		from bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '-----------------------';

		-- Loading silver.crm_sales_details
		SET @start_time = GETDATE();
		PRINT '>> Truncated Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserted Data Into: silver.crm_sales_details';
		 INSERT INTO silver.crm_sales_details (
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
		)
		 SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN sls_order_dt = 0 or LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE WHEN sls_ship_dt = 0 or LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE WHEN sls_due_dt = 0 or LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales!= sls_sales*ABS(sls_price) THEN (sls_quantity*ABS(sls_price)) 
				ELSE sls_sales 
			END as sls_sales,
			sls_quantity,
			CASE WHEN sls_price IS NULL OR sls_price<=0  THEN sls_sales/NULLIF(sls_quantity,0) 
				ELSE sls_price 
			End as sls_price
		FROM bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '-----------------------';

		PRINT '-----------------------------------------';
		PRINT 'ERP Tables Loaded';
		PRINT '-----------------------------------------';

		-- Loading silver.erp_cust_az12
		SET @start_time = GETDATE();
		PRINT '>> Truncated Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserted Data Into: silver.erp_cust_az12';
		 INSERT INTO silver.erp_cust_az12(CID,BDATE,GEN)
		 SELECT 
			CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID)) -- Remove 'NAS' prefix if present
				ELSE CID
			END AS CID,
			CASE WHEN BDATE > GETDATE() THEN NULL
				ELSE BDATE --Set future birthdays to  NULL
			END AS BDATE,
			CASE WHEN UPPER(TRIM(GEN)) IN ('F', 'Female') THEN 'Female'
				 WHEN UPPER(TRIM(GEN)) IN ('M', 'Male') THEN 'Male'
				 ELSE 'n/a'
			END AS GEN --Normalize gender values and handle unknown cases
		  FROM [bronze].[erp_cust_az12];
		  SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '-----------------------';

		-- Loading silver.erp_loc_a101
		SET @start_time = GETDATE();
		PRINT '>> Truncated Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserted Data Into: silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101 (cid, cntry)
		SELECT 
		REPLACE(CID, '-','') AS CID,
		CASE WHEN TRIM(CNTRY) = 'DE' THEN 'GERMANY'
			 WHEN TRIM(CNTRY) IN ('US','USA') THEN 'United States'
			 WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN 'n/a'
			 ELSE TRIM(CNTRY)
		END AS CNTRY -- Normalize and Handle missing or blank country codes
		FROM bronze.erp_loc_a101;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '-----------------------';

		-- Loading silver.erp_px_cat_g1v2
		SET @start_time = GETDATE();
		PRINT '>> Truncated Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserted Data Into: silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
		SELECT DISTINCT
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '-----------------------';

		SET @batch_end_time = GETDATE();
		PRINT '======================================================';
		PRINT 'Silver Layer Load Complete!';
		PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
		PRINT '======================================================';

	END TRY
	BEGIN CATCH
		PRINT '======================================================';
		PRINT 'ERROR OCCURED WHILE LOADING SILVER LAYER';
		PRINT 'Error Message:' + ERROR_MESSAGE();
		PRINT 'Error Message:' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message:' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '======================================================';
	END CATCH
END
