/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

--use DataWarehouse;
--Data Transformation before move data from bronze to silver layer
--=============================================================
create or alter procedure silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME ,@end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time =Getdate();
		print '================================================';
		print 'Loading Silver Layer';
		print '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		SET @start_time =Getdate();
		print '>>Trancating table:silver.crm_cust_info';
		Truncate table silver.crm_cust_info;
		print '>>Inserting Data into :silver.crm_cust_info';
		insert into silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)
		select 
		cst_id,
		cst_key,
		trim(cst_firstname) as cst_firstname,
		trim(cst_lasttname) as cst_lastname ,
		case when upper(trim(cst_material_status)) = 'S' then 'Single'
			 when upper(trim(cst_material_status)) = 'M' then 'Married'
			 else 'N/A'
		END cst_material_status,

		case when upper(trim(cst_gndr)) = 'F' then 'Female'
			 when upper(trim(cst_gndr)) = 'M' then 'Male'
			 else 'N/A'
		END cst_gndr,
		cst_create_date
		from(
			select * ,
			ROW_NUMBER () OVER (PARTITION BY cst_id order by cst_create_date desc) as flag_last
			from bronze.crm_cust_info
			where cst_id is not null
		)t where flag_last =1
		SET @end_time =Getdate();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
		PRINT '>> -------------';
		--=============================================================
		--bronze.crm_prd_info
		SET @start_time =Getdate();
		print '>>Trancating table:silver.crm_prd_info';
		Truncate table silver.crm_prd_info;
		print '>>Inserting Data into :silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT prd_id,
			   replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
			   SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,
			   prd_nm,
			   isnull(prd_cost,0) as prd_cost,
			   case UPPER(trim(prd_line)) 
					WHEN 'M' THEN 'Mountain'
					WHEN 'R' THEN 'Road'
					when 'S' THEN 'Other sales'
					when 'T' THEN 'Touring'
					Else 'N/A'
			   END AS prd_line,
			   cast(prd_start_dt as DATE) AS prd_start_dt,
			   cast(
					LEAD(prd_start_dt) over (partition by prd_key order by prd_start_dt desc)-1 
					AS DATE
				)AS prd_end_dt
		  FROM [DataWarehouse].[bronze].[crm_prd_info]
		SET @end_time =Getdate();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
		PRINT '>> -------------';
		--=============================================================
		--bronze.crm_sales_details
		SET @start_time =Getdate();
		print '>>Trancating table:silver.crm_sales_details';
		Truncate table silver.crm_sales_details;
		print '>>Inserting Data into :silver.crm_sales_details';
		insert into silver.crm_sales_details (
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
		SELECT sls_ord_num,
			  sls_prd_key,
			  sls_cust_id,
			  case when sls_order_dt = 0 or len(sls_order_dt) !=8 then NULL
				   else CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			  END AS sls_order_dt,

			  case when sls_ship_dt = 0 or len(sls_ship_dt) !=8 then NULL
			  else CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			  END AS sls_ship_dt,

			  case when sls_due_dt = 0 or len(sls_due_dt) !=8 then NULL
			  else CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			  END AS sls_due_dt,
      
			  case when	sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity*abs(sls_price)
						then sls_quantity *abs(sls_price)
				   else sls_sales
			  end as sls_sales,

			  sls_quantity,

			  case when	sls_price is null or sls_price <=0 
						then sls_sales /nullif(sls_quantity,0)
				   else sls_price
			  end as sls_price
		FROM DataWarehouse.bronze.crm_sales_details
		SET @end_time =Getdate();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
		PRINT '>> -------------';
		--=============================================================
		--bronze.erp_cust_az12
		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';
		SET @start_time =Getdate();
		print '>>Trancating table:silver.erp_cust_az12';
		Truncate table silver.erp_cust_az12;
		print '>>Inserting Data into :silver.erp_cust_az12';
		insert into silver.erp_cust_az12(
			cid,
			bdate,
			gen
		)
		SELECT 
			   CASE WHEN cid like 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
					ELSE cid
			   end as cid,

			   CASE WHEN bdate >GETDATE() then null
					ELSE bdate
			   end as bdate,

			   case when upper(trim(gen)) in ('F','FEMALE') then 'Female'
					when upper(trim(gen)) in ('M','MALE') then 'Male'
					ELSE 'N/A'
			   END AS gen
		FROM bronze.erp_cust_az12
		SET @end_time =Getdate();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
		PRINT '>> -------------';
		--=============================================================
		--bronze.erp_loc_a101
		SET @start_time =Getdate();
		print '>>Trancating table:silver.erp_loc_a101';
		Truncate table silver.erp_loc_a101;
		print '>>Inserting Data into :silver.erp_loc_a101';

		insert into silver.erp_loc_a101 (cid,cntry)
		SELECT  replace (cid,'-',''),
				case when trim(cntry) ='DE' then 'Germany'
					 when trim(cntry) in ('US','USA') then 'United States'
					 when trim(cntry) ='' or cntry is null then 'N/A'
					 ELSE trim(cntry)
				END AS cntry
		FROM bronze.erp_loc_a101
		SET @end_time =Getdate();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
		PRINT '>> -------------';
		--=============================================================
		--bronze.erp_px_cat_g1v2
		SET @start_time =Getdate();
		print '>>Trancating table:silver.erp_px_cat_g1v2';
		Truncate table silver.erp_px_cat_g1v2;
		print '>>Inserting Data into :silver.erp_px_cat_g1v2';

		insert into silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance
			)
		select id,cat,subcat,maintenance
		FROM bronze.erp_px_cat_g1v2
		SET @end_time =Getdate();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' seconds';
		PRINT '>> -------------';
		SET @batch_end_time =Getdate();
		PRINT 'Loading Silver Layer is Completed'; 
		PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END 
EXEC silver.load_silver;
