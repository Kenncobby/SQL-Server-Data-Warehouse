/*
=============================================================================================

Stored Procedure: Load Bronze Layer (Source -> Bronze)

=============================================================================================
Script Purpose:
	This stored procedure loads data into the 'bronze' layer/schema of the DataWarehouse from
	external CSV files. It preforms the following actions:
	- Truncates the bronze tables before loading data.
	- Uses the 'BULK INSERT' command to lad data from CSV files into the bronze schema tables

Parameters:
	@dataset_path NVARCHAR(400) = N'C:\DRIVE D\SQL-Server-Data-Warehouse\datasets'
	The root folder containing the source_crm and source_erp subfolders. Defaults to the
	datasets folder in this repo; pass a different path if your CSVs live elsewhere.

Usage Example:
	EXEC bronze.load_bronze;
	EXEC bronze.load_bronze @dataset_path = N'C:\some\other\path\datasets';
=============================================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze
	@dataset_path NVARCHAR(400) = N'C:\DRIVE D\SQL-Server-Data-Warehouse\datasets'
AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	DECLARE @sql NVARCHAR(MAX);
	DECLARE @safe_path NVARCHAR(400) = REPLACE(@dataset_path, '''', '''''');
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '=========================================';
		PRINT 'Bronze Layer Loaded';
		PRINT '=========================================';

		PRINT '-----------------------------------------';
		PRINT 'CRM Tables Loaded';
		PRINT '-----------------------------------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncated Table: bronze.crm_cust_info <<';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserted Data Into: bronze.crm_cust_info <<';
		SET @sql = N'BULK INSERT bronze.crm_cust_info FROM ''' + @safe_path + N'\source_crm\cust_info.csv'' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK);';
		EXEC sp_executesql @sql;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '-----------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncated Table: bronze.crm_prd_info <<';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserted Data Into: bronze.crm_prd_info <<';
		SET @sql = N'BULK INSERT bronze.crm_prd_info FROM ''' + @safe_path + N'\source_crm\prd_info.csv'' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK);';
		EXEC sp_executesql @sql;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '-----------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncated Table: crm_sales_details <<';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserted Data Into: crm_sales_details <<';
		SET @sql = N'BULK INSERT bronze.crm_sales_details FROM ''' + @safe_path + N'\source_crm\sales_details.csv'' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK);';
		EXEC sp_executesql @sql;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '-----------------------';

		PRINT '-----------------------------------------';
		PRINT 'ERP Tables Loaded';
		PRINT '-----------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncated Table: bronze.erp_cust_az12 <<';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserted Data Into: bronze.erp_cust_az12 <<';
		SET @sql = N'BULK INSERT bronze.erp_cust_az12 FROM ''' + @safe_path + N'\source_erp\CUST_AZ12.csv'' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK);';
		EXEC sp_executesql @sql;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '-----------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncated Table: erp_loc_a101 <<';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserted Data Into: bronze.erp_loc_a101 <<';
		SET @sql = N'BULK INSERT bronze.erp_loc_a101 FROM ''' + @safe_path + N'\source_erp\LOC_A101.csv'' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK);';
		EXEC sp_executesql @sql;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '-----------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncated Table: bronze.erp_px_cat_g1v2 <<';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserted Data Into: bronze.erp_px_cat_g1v2 <<';
		SET @sql = N'BULK INSERT bronze.erp_px_cat_g1v2 FROM ''' + @safe_path + N'\source_erp\PX_CAT_G1V2.csv'' WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK);';
		EXEC sp_executesql @sql;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '-----------------------';

		SET @batch_end_time = GETDATE();
		PRINT '======================================================';
		PRINT 'Bronze Layer Load Complete!';
		PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
		PRINT '======================================================';
	END TRY
	BEGIN CATCH
		PRINT '======================================================';
		PRINT 'ERROR OCCURED WHILE LOADING BRONZE LAYER';
		PRINT 'Error Message:' + ERROR_MESSAGE();
		PRINT 'Error Message:' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message:' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '======================================================';
	END CATCH
END
