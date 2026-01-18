USE DataWarehouse
GO

CREATE OR ALTER PROCEDURE bronze.spLOAD_BRONZE AS
/*
Load data to bronze layer from target (CSV files)
  
Modyfication history:
	2026-01-18 KO Init
*/
BEGIN
	BEGIN TRY
		
		DECLARE @batch_start_time DATETIME
		DECLARE @batch_end_time DATETIME
		DECLARE @end_time DATETIME
		DECLARE @start_time DATETIME

		SET @batch_start_time = GETDATE()

		PRINT '===================================';
		PRINT '#####   Loading Bronze Layer   ####';
		PRINT '===================================';


		PRINT '===================================';
		PRINT '####    Loading CRM Tables     ####';
		PRINT '===================================';

		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.crmCUST_INFO';
		TRUNCATE TABLE bronze.crmCUST_INFO;

		PRINT '>> Inserting Data Into: bronze.crmCUST_INFO';
		BULK INSERT bronze.crmCUST_INFO
			FROM 'C:\Users\osins\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
		SET @end_time = GETDATE()
		PRINT 'Duration: ' + CAST(DATEDIFF(MILLISECOND,@start_time,@end_time) AS VARCHAR) + ' milliseconds';


		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.crmPRD_INFO';
		TRUNCATE TABLE bronze.crmPRD_INFO

		PRINT '>> Inserting Data Into: bronze.crmPRD_INFO';
		BULK INSERT bronze.crmPRD_INFO
			FROM 'C:\Users\osins\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
		SET @end_time = GETDATE()
		PRINT 'Duration: ' + CAST(DATEDIFF(MILLISECOND,@start_time,@end_time) AS VARCHAR) + ' milliseconds';


		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.crmSALES_DETAIL';
		TRUNCATE TABLE bronze.crmSALES_DETAILS

		PRINT '>> Inserting Data Into: bronze.crmSALES_DETAIL';
		BULK INSERT bronze.crmSALES_DETAILS
			FROM 'C:\Users\osins\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
		SET @end_time = GETDATE()
		PRINT 'Duration: ' + CAST(DATEDIFF(MILLISECOND,@start_time,@end_time) AS VARCHAR) + ' milliseconds';


		PRINT '===================================';
		PRINT '####    Loading ERP Tables     ####';
		PRINT '===================================';


		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.erpCUST_AZ12';
		TRUNCATE TABLE bronze.erpCUST_AZ12;

		PRINT '>> Inserting Data Into: bronze.erpCUST_AZ12';
		BULK INSERT bronze.erpCUST_AZ12
			FROM 'C:\Users\osins\Desktop\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
		SET @end_time = GETDATE()
		PRINT 'Duration: ' + CAST(DATEDIFF(MILLISECOND,@start_time,@end_time) AS VARCHAR) + ' milliseconds';


		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.erpLOC_A101';
		TRUNCATE TABLE bronze.erpLOC_A101;

		PRINT '>> Inserting Data Into: bronze.erpLOC_A101';
		BULK INSERT bronze.erpLOC_A101
			FROM 'C:\Users\osins\Desktop\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			);
		SET @end_time = GETDATE()
		PRINT 'Duration: ' + CAST(DATEDIFF(MILLISECOND,@start_time,@end_time) AS VARCHAR) + ' milliseconds';


		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: bronze.erpCAT_G1V2';
		TRUNCATE TABLE bronze.erpCAT_G1V2

		PRINT '>> Inserting Data Into: bronze.erpCAT_G1V2';
		BULK INSERT bronze.erpCAT_G1V2
			FROM 'C:\Users\osins\Desktop\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
			WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
			)
		SET @end_time = GETDATE()
		PRINT 'Duration: ' + CAST(DATEDIFF(MILLISECOND,@start_time,@end_time) AS VARCHAR) + ' milliseconds';

		SET @batch_end_time = GETDATE()
		
		
		PRINT '==========================================================';
		PRINT 'Batch Duration: ' + CAST(DATEDIFF(MILLISECOND,@batch_start_time,@batch_end_time) AS VARCHAR) + ' milliseconds';
		PRINT '==========================================================';

	END TRY
	BEGIN CATCH 
		PRINT '==========================================================';
		PRINT '#### ERROR OCCURED DURING LOADING BRONZE LATER';   
		PRINT '####    >>  MSG: ' + ERROR_MESSAGE();
		PRINT '####    >> NUMBER: ' + CAST(ERROR_NUMBER() AS VARCHAR);
		PRINT '####    >> STATE: ' + CAST(ERROR_STATE() AS VARCHAR);
		PRINT '==========================================================';
		THROW
	END CATCH
END 
GO
