CREATE OR ALTER VIEW gold.dimCUSTOMERS AS 
/*
Modyfication history:
	2026-01-18 KO Init
*/
SELECT
	ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS custorem_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	ca.bdate AS birth,
	ci.cst_create_date AS create_date,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
	ELSE ISNULL(ca.gen,'n/a') END AS gender
FROM silver.crmCUST_INFO ci
LEFT JOIN silver.erpCUST_AZ12 ca ON ci.cst_key = ca.cid
LEFT JOIN silver.erpLOC_A101 la ON ci.cst_key = la.cid
GO


CREATE OR ALTER VIEW gold.dimPRODUCTS AS
/*
Modyfication history:
	2026-01-18 KO Init
*/
SELECT 
	  ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS products_key,
	  pn.prd_id AS product_id
	 ,pn.prd_key AS product_number
	 ,pn.prd_nm AS product_name
	 ,pn.cat_id AS category_id
	 ,pc.cat AS category
	 ,pc.subcat AS subcategory
	 ,pc.maintenance 
	 ,pn.prd_cost AS cost
	 ,pn.prd_line AS product_line
	 ,pn.prd_start_dt AS start_date
FROM silver.crmPRD_INFO pn
LEFT JOIN silver.erpCAT_G1V2 pc ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL
GO


CREATE OR ALTER VIEW gold.factSALES AS
/*
Modyfication history:
	2026-01-18 KO Init
*/
SELECT 
       sd.sls_ord_num AS order_number
	  ,pr.products_key
      ,cu.customer_key
      ,sd.sls_order_dt AS order_date
      ,sd.sls_ship_dt AS shipping_date
      ,sd.sls_due_dt AS due_date
      ,sd.sls_sales AS sales_amount
      ,sd.sls_quantity AS quantity
      ,sd.sls_price
FROM silver.crmSALES_DETAILS sd
LEFT JOIN gold.dimPRODUCTS pr ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dimCUSTOMERS cu ON sd.sls_cust_id = cu.customer_id
GO