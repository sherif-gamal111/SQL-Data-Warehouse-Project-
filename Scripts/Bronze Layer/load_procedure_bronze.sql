/*
===================================================================================================================================================================
                                                      Stored Procedure: Load Bronze Layer with the data from the source system
===================================================================================================================================================================
Description:
    The stored procedure loads data from the source system's CSV files into the bronze schema.
    It performs the following actions:
        1- Truncate the bronze tables before loading data.
        2- Uses BULK INSERT to load data from the CSV Files to the bronze tables
        3- Measures the time that data takes to be loaded into the tables.

    Parameters:
        None --> this stored procedure does not take any parameters or return any values.

    Usage Example:
        EXEC Bronze.load_bronze;
===================================================================================================================================================================
*/

exec Bronze.load_bronze;

create or alter procedure Bronze.load_bronze as 
begin

	declare @start_time datetime , @end_time datetime , @batch_start_time datetime , @batch_end_time datetime;

	begin try

		set @batch_start_time = getdate();

		print'============================================================================================================================================================================='
		print'                                                                   Loading Bronze Layer                                                                                      '
		print'============================================================================================================================================================================='

		print'-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------'
		print'                                                                   Loading CRM Layer                                                                                         '
		print'-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------'

		set @start_time = getdate(); 
		print'Truncating table: Bronze.crm_cust_info';
		truncate table Bronze.crm_cust_info;

		print'Inserting data into table: Bronze.crm_cust_info';
		bulk insert Bronze.crm_cust_info
		from 'D:\Important\Programming\Data\Data Engineering\Projects\Baraa\SQL Data Warehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock 
		);

		set @end_time = getdate(); -- to know what time the data loading end
		print'File Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds' -- to know how much time the data loading takes
		
		select * 
		from Bronze.crm_cust_info;

		print'--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'

		print'Truncating table: Bronze.crm_prd_info';
		set @start_time = getdate(); -- to know what time the data loading start
		truncate table Bronze.crm_prd_info;

		print'Inserting data into table: Bronze.crm_prd_info';
		bulk insert Bronze.crm_prd_info
		from 'D:\Important\Programming\Data\Data Engineering\Projects\Baraa\SQL Data Warehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock 
		);

		set @end_time = getdate(); 
		print'File Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds' 
	
		select * 
		from Bronze.crm_prd_info;

		print'--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'

		print'Truncating table: Bronze.crm_sales_details';
		set @start_time = getdate(); 
		truncate table Bronze.crm_sales_details;

		print'Inserting data into table: Bronze.crm_sales_details';
		bulk insert Bronze.crm_sales_details
		from 'D:\Important\Programming\Data\Data Engineering\Projects\Baraa\SQL Data Warehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock 
		);

		set @end_time = getdate(); 
		print'File Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds' 
	
		select * 
		from Bronze.crm_sales_details;

		print'-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------'
		print'                                                                   Loading ERP Layer                                                                                         '
		print'-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------'


		print'Truncating table: Bronze.erp_cust_az12';
		set @start_time = getdate(); -- to know what time the data loading start
		truncate table Bronze.erp_cust_az12;

		print'Inserting data into table: Bronze.erp_cust_az12';
		bulk insert Bronze.erp_cust_az12
		from 'D:\Important\Programming\Data\Data Engineering\Projects\Baraa\SQL Data Warehouse\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock 
		);

		set @end_time = getdate(); 
		print'File Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds' 
	
		select *
		from Bronze.erp_cust_az12;

		print'--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'

		print'Truncating table: Bronze.erp_loc_a101';
		set @start_time = getdate();
		truncate table Bronze.erp_loc_a101;

		print'Inserting data into table: Bronze.erp_loc_a101';
		bulk insert Bronze.erp_loc_a101
		from 'D:\Important\Programming\Data\Data Engineering\Projects\Baraa\SQL Data Warehouse\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock 
		);

		set @end_time = getdate(); 
		print'File Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
	
		select * 
		from Bronze.erp_loc_a101;

		print'--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------'

		print'Truncating table: Bronze.erp_px_cat_g1v2';
		set @start_time = getdate(); 
		truncate table Bronze.erp_px_cat_g1v2;

		print'Inserting data into table: Bronze.erp_px_cat_g1v2';
		bulk insert Bronze.erp_px_cat_g1v2
		from 'D:\Important\Programming\Data\Data Engineering\Projects\Baraa\SQL Data Warehouse\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock 
		);

		set @end_time = getdate(); 
		print'File Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds' 
	
		select * 
		from Bronze.erp_px_cat_g1v2;

		set @batch_end_time = getdate();
		print'Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print'============================================================================================================================================================================'
		print'Loading Bronze Layer Is Compeleted';
		print'Total Batch Load Duration is: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + ' seconds'
		print'============================================================================================================================================================================'

	end try


	begin catch
		print'============================================================================================================================================================================'
		print'ERROR OCCURED DURING  LOADING BRONZE LAYER';
		print'ERROR MESSAGE' + ERROR_MESSAGE();
		print'ERROR MESSAGE' + CAST (ERROR_MESSAGE() AS NVARCHAR);
		print'ERROR MESSAGE' + CAST (ERROR_STATE() AS NVARCHAR);
		print'============================================================================================================================================================================'
	end catch

end
