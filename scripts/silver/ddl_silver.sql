/*
  DDL Script: Create Silver Tables
*/

USE DataWarehouse
GO

  
IF NOT EXISTS (SELECT * FROM sys.tables T INNER JOIN sys.schemas S ON T.schema_id = S.schema_id WHERE t.name = 'crmCUST_INFO' AND s.name = 'bronze')
CREATE TABLE silver.crmCUST_INFO (
	cst_id			   INTEGER,
	cst_key			   NVARCHAR(50),
	cst_firstname	   NVARCHAR(50),
	cst_lastname	   NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gndr           NVARCHAR(50),
	cst_create_date    DATE,
    dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
GO


IF NOT EXISTS (SELECT * FROM sys.tables T INNER JOIN sys.schemas S ON T.schema_id = S.schema_id WHERE t.name = 'crmPRD_INFO' AND s.name = 'bronze')
CREATE TABLE silver.crmPRD_INFO (
	prd_id       INTEGER,
	prd_key      NVARCHAR(50),
	prd_nm       NVARCHAR(50),
	prd_cost     INTEGER,
	prd_line     NVARCHAR(50),
	prd_start_dt DATETIME,
	prd_end_dt   DATETIME,
    dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
GO

  
IF NOT EXISTS (SELECT * FROM sys.tables T INNER JOIN sys.schemas S ON T.schema_id = S.schema_id WHERE t.name = 'crmSALES_DETAILS' AND s.name = 'bronze')
CREATE TABLE silver.crmSALES_DETAILS (
	sls_ord_num  NVARCHAR(50),
	sls_prd_key  NVARCHAR(50),
	sls_cust_id  INTEGER,
	sls_order_dt INTEGER,
	sls_ship_dt  INTEGER,
	sls_due_dt   INTEGER,
	sls_sales    INTEGER,
	sls_quantity INTEGER,
	sls_price    INTEGER,
    dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
GO


IF NOT EXISTS (SELECT * FROM sys.tables T INNER JOIN sys.schemas S ON T.schema_id = S.schema_id WHERE t.name = 'erpCUST_AZ12' AND s.name = 'bronze')
CREATE TABLE silver.erpCUST_AZ12 (
	cid	  NVARCHAR(50),
	bdate DATE,
	gen   NVARCHAR(50),
	dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
GO

  
IF NOT EXISTS (SELECT * FROM sys.tables T INNER JOIN sys.schemas S ON T.schema_id = S.schema_id WHERE t.name = 'erpLOC_A101' AND s.name = 'bronze')
CREATE TABLE silver.erpLOC_A101 (
	cid	  NVARCHAR(50),
	cntry NVARCHAR(50),
    dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
GO


IF NOT EXISTS (SELECT * FROM sys.tables T INNER JOIN sys.schemas S ON T.schema_id = S.schema_id WHERE t.name = 'erpCAT_G1V2' AND s.name = 'bronze')
CREATE TABLE silver.erpCAT_G1V2 (
	id			NVARCHAR(50),
	cat		    NVARCHAR(50),
	subcat		NVARCHAR(50),
	maintenance NVARCHAR(50),
    dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
GO
