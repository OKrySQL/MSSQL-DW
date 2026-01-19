CREATE OR ALTER PROCEDURE silver.spLOAD_SILVER AS
/*
Load data to silver layer from bronze layer
  
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
		PRINT '#####   Loading Silver Layer   ####';
		PRINT '===================================';


		PRINT '===================================';
		PRINT '####    Loading CRM Tables     ####';
		PRINT '===================================';


		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: silver.crmCUST_INFO';
		TRUNCATE TABLE silver.crmCUST_INFO;

		PRINT '>> Inserting Data Into: silver.crmCUST_INFO';
        INSERT INTO silver.crmCUST_INFO (cst_id, cst_key , cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date) 
			SELECT cst_id, cst_key, TRIM(cst_firstname) AS cst_firstname, TRIM(cst_lastname) AS cst_lastname,
            
				CASE WHEN UPPER(TRIM(cst_marital_status))  = 'S' THEN 'Single'
					 WHEN UPPER(TRIM(cst_marital_status))  = 'M' THEN 'Married' 
					 ELSE 'n/a' 
				END AS cst_marital_status,
            
				CASE WHEN UPPER(TRIM(cst_gndr))  = 'F' THEN 'Female'
					 WHEN UPPER(TRIM(cst_gndr))  = 'M' THEN 'Male' 
					 ELSE 'n/a' 
				END AS cst_gndr,
				cst_create_date 
			FROM (
				SELECT  ROW_NUMBER() OVER(PARTITION BY cst_id order by cst_create_date DESC) AS last_date, *
					FROM bronze.crmCUST_INFO
					WHERE cst_id IS NOT NULL
				) AS cust_info 
			  WHERE cust_info.last_date = 1 

		SET @end_time = GETDATE()
		PRINT 'Duration: ' + CAST(DATEDIFF(MILLISECOND,@start_time,@end_time) AS VARCHAR) + ' milliseconds';


		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: silver.crmPRD_INFO';
		TRUNCATE TABLE silver.crmPRD_INFO

		PRINT '>> Inserting Data Into: silver.crmPRD_INFO';
        INSERT INTO silver.crmPRD_INFO (prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
			SELECT  prd_id,      
		            REPLACE(SUBSTRING(prd_key, 1, 5),'-','_') AS cat_id,
		            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
		            prd_nm,
		            ISNULL(prd_cost,0) AS prd_cost,   
		            CASE UPPER(TRIM(prd_line))
				         WHEN 'M' THEN 'Mountain'
				         WHEN 'R' THEN 'Road'
				         WHEN 'S' THEN 'Other Sales'
		                 WHEN 'T' THEN 'Touring'
						 ELSE 'n/a'
				   END AS prd_line,
		           CAST(prd_start_dt AS DATE),
		           CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE) AS prd_end_dt
	        FROM bronze.crmPRD_INFO 

		SET @end_time = GETDATE()
		PRINT 'Duration: ' + CAST(DATEDIFF(MILLISECOND,@start_time,@end_time) AS VARCHAR) + ' milliseconds';


		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: silver.crmSALES_DETAIL';
		TRUNCATE TABLE silver.crmSALES_DETAILS

		PRINT '>> Inserting Data Into: silver.crmSALES_DETAIL';
	    INSERT INTO  silver.crmSALES_DETAILS (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
			SELECT
				  sls_ord_num,
				  sls_prd_key,
				  sls_cust_id,

				  CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
					   ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
				  END AS sls_order_dt,

				  CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
					   ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
				  END AS sls_ship_dt,

				  CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
					   ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
				  END AS sls_due_dt,

				  CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price) 
					   ELSE sls_sales
				  END AS sls_sales,

				  CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0) 
					   ELSE sls_price
				  END AS sls_price,
				  sls_quantity
			  FROM bronze.crmSALES_DETAILS

		SET @end_time = GETDATE()
		PRINT 'Duration: ' + CAST(DATEDIFF(MILLISECOND,@start_time,@end_time) AS VARCHAR) + ' milliseconds';


		PRINT '===================================';
		PRINT '####    Loading ERP Tables     ####';
		PRINT '===================================';

  
  		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: silver.erpCUST_AZ12';
		TRUNCATE TABLE silver.erpCUST_AZ12;

		PRINT '>> Inserting Data Into: silver.erpCUST_AZ12';
        INSERT INTO silver.erpCUST_AZ12 (cid, bdate, gen)
			SELECT 
				  CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
					ELSE cid 
				  END AS cid,
              
				  CASE WHEN bdate > GETDATE() THEN NULL
					ELSE bdate 
				  END AS bdate,
              
				  CASE TRIM(UPPER(gen))
					 WHEN 'F'      THEN 'Fi\emale'
					 WHEN 'FEMALE' THEN gen
					 WHEN 'MALE'   THEN gen
					 WHEN 'M'      THEN 'Male'
					 ELSE 'n/a'
				  END AS gen
			FROM bronze.erpCUST_AZ12

		SET @end_time = GETDATE()
		PRINT 'Duration: ' + CAST(DATEDIFF(MILLISECOND,@start_time,@end_time) AS VARCHAR) + ' milliseconds';

		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: silver.erpLOC_A101';
		TRUNCATE TABLE silver.erpLOC_A101;

        PRINT '>> Inserting Data Into: silver.erpLOC_A101';
        INSERT INTO silver.erpLOC_A101 (cid, cntry)
			SELECT 
				  REPLACE(cid,'-','') AS cid,
				  CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
					   WHEN TRIM(cntry) in ('US', 'USA') THEN 'United States'
					   WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
					   ELSE cntry 
				  END AS cntry
			FROM bronze.erpLOC_A101


		SET @end_time = GETDATE()
		PRINT 'Duration: ' + CAST(DATEDIFF(MILLISECOND,@start_time,@end_time) AS VARCHAR) + ' milliseconds';


		SET @start_time = GETDATE()
		PRINT '>> Truncating Table: silver.erpCAT_G1V2';
		TRUNCATE TABLE silver.erpCAT_G1V2


		PRINT '>> Inserting Data Into: silver.erpCAT_G1V2';
          INSERT INTO silver.erpCAT_G1V2 (id, cat, subcat, maintenance)
			  SELECT 
				   id
				  cat,
				  subcat,
				  maintenance
			  FROM bronze.erpCAT_G1V2

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