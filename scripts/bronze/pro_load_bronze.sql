/*
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables
*/


create or alter procedure load_bronze as
begin
    
	declare @start_load datetime , @end_load datetime , @start_batch_load datetime ,  @end_batch_load datetime;
	begin try
		set @start_batch_load =getdate();

		PRINT '================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '================================================';
	
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		set @start_load = getdate();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		truncate table bronze.crm_cust_info ;
		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'D:\DWH_Project\datasets\source_crm\cust_info.csv'
		with(
		firstrow = 2,
		fieldterminator = ',',
		tablock 
		);
		set @end_load = getdate();
		PRINT '>> Load time: ' + CAST(DATEDIFF(second, @start_load, @end_load) AS NVARCHAR) + ' seconds';
		PRINT ' -------------';


		set @start_load = getdate();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		truncate table bronze.crm_prd_info ; 
		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		bulk insert bronze.crm_prd_info 
		from 'D:\DWH_Project\datasets\source_crm\prd_info.csv'
		with(
		firstrow = 2 ,
		fieldterminator = ',',
		tablock 
		);
		set @end_load = getdate();
		PRINT '>> Load time: ' + CAST(DATEDIFF(second, @start_load, @end_load) AS NVARCHAR) + ' seconds';
		PRINT ' -------------';



		set @start_load = getdate();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		truncate table bronze.crm_sales_details ;
		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details 
		from 'D:\DWH_Project\datasets\source_crm\sales_details.csv'
		with(
		firstrow = 2 ,
		fieldterminator = ',',
		tablock
		);
		set @end_load = getdate();
		PRINT '>> Load time: ' + CAST(DATEDIFF(second, @start_load, @end_load) AS NVARCHAR) + ' seconds';
		PRINT ' -------------';



		set @start_load = getdate();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		truncate table bronze.erp_cust_az12 ;
		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		bulk insert bronze.erp_cust_az12
		from 'D:\DWH_Project\datasets\source_erp\CUST_AZ12.csv'
		with(
		firstrow = 2 ,
		fieldterminator = ',',
		tablock
		);
		set @end_load = getdate();
		PRINT '>> Load time: ' + CAST(DATEDIFF(second, @start_load, @end_load) AS NVARCHAR) + ' seconds';
		PRINT ' -------------';




		set @start_load = getdate();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		truncate table bronze.erp_loc_a101 ;
		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		bulk insert bronze.erp_loc_a101
		from 'D:\DWH_Project\datasets\source_erp\LOC_A101.csv'
		with(
		firstrow = 2,
		fieldterminator = ',',
		tablock
		);
		set @end_load = getdate();
		PRINT '>> Load time: ' + CAST(DATEDIFF(second, @start_load, @end_load) AS NVARCHAR) + ' seconds';
		PRINT ' -------------';




		set @start_load = getdate();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2 ;
		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		bulk insert bronze.erp_px_cat_g1v2
		from 'D:\DWH_Project\datasets\source_erp\PX_CAT_G1V2.csv'
		with(
		firstrow = 2 ,
		fieldterminator = ',',
		tablock
		);
		PRINT '>> Load time: ' + CAST(DATEDIFF(second, @start_load, @end_load) AS NVARCHAR) + ' seconds';
		PRINT ' -------------';



		set @end_batch_load = getdate();

		PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
		PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @start_batch_load, @end_batch_load) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
    end try
	begin catch
	    PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	end catch 
end
