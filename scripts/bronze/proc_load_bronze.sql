CREATE OR ALTER PROCEDURE bronze.load_bronze AS
	BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME

	BEGIN TRY
	SET @batch_start_time=GETDATE();
		PRINT '============================================================';
		PRINT 'LOADING THE BRONZE LAYER';
		PRINT '============================================================';

		PRINT '----------------------------------------------------------';
		PRINT 'LOADING THE CRM TABLES';
		PRINT '----------------------------------------------------------';
		SET @start_time =GETDATE();

		TRUNCATE TABLE bronze.crm_cust_info;

		BULK INSERT bronze.crm_cust_info
		FROM 'D:\projectt\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
		   FIRSTROW=2,
		   FIELDTERMINATOR=',',
		   TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>> Load duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' SECONDS';
		PRINT '--------------------------------------------------------------'
		--select * from bronze.crm_cust_info;
		--select COUNT(*) from bronze.crm_cust_info;

		SET @start_time =GETDATE();

		TRUNCATE TABLE bronze.crm_prd_info;

		BULK INSERT bronze.crm_prd_info
		FROM 'D:\projectt\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
		   FIRSTROW=2,
		   FIELDTERMINATOR=',',
		   TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>> Load duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' SECONDS';
		PRINT '--------------------------------------------------------------'

		--select * from bronze.crm_prd_info;
		--select COUNT(*) from bronze.crm_prd_info;

		SET @start_time =GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details;

		BULK INSERT bronze.crm_sales_details
		FROM 'D:\projectt\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
		   FIRSTROW=2,
		   FIELDTERMINATOR=',',
		   TABLOCK
		);

		SET @end_time=GETDATE();
		PRINT '>> Load duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' SECONDS';
		PRINT '--------------------------------------------------------------'
		--select * from bronze.crm_sales_details;
		--select COUNT(*) from bronze.crm_sales_details;

		PRINT '----------------------------------------------------------';
		PRINT 'LOADING THE ERP TABLES';
		PRINT '----------------------------------------------------------';
		SET @start_time =GETDATE();
		TRUNCATE TABLE bronze.erp_cust_az12;

		BULK INSERT  bronze.erp_cust_az12
		FROM 'D:\projectt\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
		   FIRSTROW=2,
		   FIELDTERMINATOR=',',
		   TABLOCK
		);

		SET @end_time=GETDATE();
		PRINT '>> Load duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' SECONDS';
		PRINT '--------------------------------------------------------------'
		--select * from  bronze.erp_cust_az12;
		--select COUNT(*) from  bronze.erp_cust_az12;
		SET @start_time =GETDATE();

		TRUNCATE TABLE bronze.erp_loc_a101;

		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\projectt\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
		   FIRSTROW=2,
		   FIELDTERMINATOR=',',
		   TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>> Load duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' SECONDS';
		PRINT '--------------------------------------------------------------'

		--select * from bronze.erp_loc_a101;
		--select COUNT(*) from bronze.erp_loc_a101;

		SET @start_time =GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\projectt\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
		   FIRSTROW=2,
		   FIELDTERMINATOR=',',
		   TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>> Load duration: '+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)+' SECONDS';
		PRINT '--------------------------------------------------------------'
		--select * from bronze.erp_px_cat_g1v2;
		--select COUNT(*) from bronze.erp_px_cat_g1v2;
		SET @batch_end_time=GETDATE();

		PRINT 'LOADING BROZE LAYER IS COMPLETED'
		PRINT '  - Total Duration : '+ CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time)AS NVARCHAR)+'seconds'

	END TRY
	BEGIN CATCH
		PRINT '================================================='
		PRINT ' ERROR OCCURED  DURING LOADING BRONZE LAYER'
		PRINT 'ERROR MESSAGE :'+ERROR_MESSAGE();
		PRINT 'ERROR NUMBER : ' +CAST( ERROR_NUMBER()AS NVARCHAR);
		PRINT 'ERROR STATE : ' +CAST( ERROR_STATE()AS NVARCHAR);
		PRINT '=================================================='
	END CATCH

END
