/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses command to load data from csv Files to bronze tables.
===============================================================================
*/

create or replace function bronze.load_bronze_log_only()
returns void
language plpgsql
as $$
declare 
	start_time timestamp;
	end_time timestamp;
	batch_start_time timestamp;
	batch_end_time timestamp;
	duration_time int;
begin
	batch_start_time:= clock_timestamp();
	raise notice '=================================';
	raise notice 'Loading Bronze Layer';
	raise notice '=================================';

	raise notice '--- Loading CRM Tables ---';
	
	--crm_cust_info
	
	start_time:= clock_timestamp();
	raise notice '>> Truncating Table: bronze.crm_cust_info';
	TRUNCATE TABLE bronze.crm_cust_info;
	raise notice'>> Importing into: bronze.crm_cust_info';
	end_time:= clock_timestamp();
	duration_time := EXTRACT (EPOCH FROM (end_time - start_time))::int;
	raise notice '>> Loading Duration: % seconds', duration_time;
	raise notice '>>-------------------------------';
	
	--crm_prd_info
	
	start_time:= clock_timestamp();
	raise notice '>> Truncating Table: bronze.crm_prd_info';
	TRUNCATE TABLE bronze.crm_prd_info;
	raise notice'>> Importing into: bronze.prd_info';
	end_time:= clock_timestamp();
	duration_time := EXTRACT (EPOCH FROM (end_time - start_time))::int;
	raise notice '>> Loading Duration: % seconds', duration_time;
	raise notice '>>-------------------------------';
	
	--crm_sales_details
	
	start_time:= clock_timestamp();
	raise notice '>> Truncating Table: bronz.crm_sales_details';
	TRUNCATE TABLE bronze.crm_sales_details;
	raise notice'>> Importing into: bronze.crm_sales_details';
	end_time:= clock_timestamp();
	duration_time := EXTRACT (EPOCH FROM (end_time - start_time))::int;
	raise notice '>> Loading Duration: % seconds', duration_time;
	raise notice '>>-------------------------------';
	
	raise notice '--- Loading ERP Tables ---';
	
	--erp_cust_az12
	
	start_time:= clock_timestamp();
	raise notice '>> Truncating Table: bronze.erp_cust_az12';
	TRUNCATE TABLE bronze.erp_cust_az12;
	raise notice'>> Importing into: bronze.erp_cust_az12';
	end_time:= clock_timestamp();
	duration_time := EXTRACT (EPOCH FROM (end_time - start_time))::int;
	raise notice '>> Loading Duration: % seconds', duration_time;
	raise notice '>>-------------------------------';
	
	--erp_loc_a101
	
	start_time:= clock_timestamp();
	raise notice '>> Truncating Table: bronze.erp_loc_a101';
	TRUNCATE TABLE bronze.erp_loc_a101;
	raise notice'>> Importing into: bronze.erp_loc_a101';
	end_time:= clock_timestamp();
	duration_time := EXTRACT (EPOCH FROM (end_time - start_time))::int;
	raise notice '>> Loading Duration: % seconds', duration_time;
	raise notice '>>-------------------------------';
	
	--erp_px_cat_g1v2
	
	start_time:= clock_timestamp();
	raise notice '>> Truncating Table: bronze.erp_px_cat_g1v2';
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	raise notice'>> Importing into: bronze.erp_px_cat_g1v2';
	end_time:= clock_timestamp();
	duration_time := EXTRACT (EPOCH FROM (end_time - start_time))::int;
	raise notice '>> Loading Duration: % seconds', duration_time;
	raise notice '>>-------------------------------';
	
	batch_end_time:= clock_timestamp();
	raise notice '==================================';
	raise notice 'Loading Broze Layer is Completed';
	raise notice '==================================';
	
	exception 
		when others then 
		--output error
		raise notice '==================================';
		raise notice 'Error Occurred During Loading Bronze Layer ';
		raise notice 'SQLERRM: %', SQLERRM;
		raise notice 'SQLSTATE: %', SQLSTATE;
		raise notice '==================================';
end;
$$

SELECT bronze.load_bronze_log_only();
	
