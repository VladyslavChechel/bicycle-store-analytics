/*
=============================================================
Create Tables
=============================================================

*/
create table bronze.crm_cust_info (
	cst_id int primary key,
	cst_key varchar(50),
	cst_firtsname varchar(50) not null,
	cst_lastname varchar(50) not null,
	cst_material_status varchar(50),
	cst_gndr varchar(50),
	cst_create_date date not null
);

create table bronze.crm_prd_info(
	prd_id int primary key,
	prd_key varchar(50) not null,
	prd_nm varchar(50) not null,
	prd_cost int not null,
	prd_line varchar(50),
	prd_start_dt date not null,
	prd_end_dt date not null 
);

create table bronze.crm_sales_details(
	sls_ord_num varchar(50) primary key,
	sls_prd_key varchar(50) not null,
	sls_cust_id int not null,
	sls_order_dt varchar(8) not null,
	sls_ship_dt varchar(8) not null,
	sls_due_dt varchar(8) not null,
	sls_sales numeric(10, 2) not null,
	sls_quantity int not null,
	sls_price numeric(10, 2) not null
);

create table bronze.erp_loc_a101(
	cid varchar(50) primary key,
	cntry varchar(50) not null 
)

create table bronze.erp_cust_az12(
	cid varchar(50) primary key,
	bdate date not null,
	gen varchar(50) not null
);

create table bronze.erp_px_cat_g1v2(
  id varchar(50) primary key,
  cat varchar(50) not null,
  subcat varchar(50) not null,
  maintenance varchar(50) not null
);



/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
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
	-- 'TRUNCATE TABLE bronze.crm_cust_info';
	raise notice'>> Importing into: bronze.crm_cust_info';
	end_time:= clock_timestamp();
	duration_time := EXTRACT (EPOCH FROM (end_time - start_time))::int;
	raise notice '>> Loading Duration: % seconds', duration_time;
	raise notice '>>-------------------------------';
	
	--crm_prd_info
	
	start_time:= clock_timestamp();
	raise notice '>> Truncating Table: bronze.crm_prd_info';
	-- 'TRUNCATE TABLE bronze.crm_prd_info';
	raise notice'>> Importing into: bronze.prd_info';
	end_time:= clock_timestamp();
	duration_time := EXTRACT (EPOCH FROM (end_time - start_time))::int;
	raise notice '>> Loading Duration: % seconds', duration_time;
	raise notice '>>-------------------------------';
	
	--crm_sales_details
	
	start_time:= clock_timestamp();
	raise notice '>> Truncating Table: bronz.crm_sales_details';
	-- 'TRUNCATE TABLE bronze.crm_sales_details';
	raise notice'>> Importing into: bronze.crm_sales_details';
	end_time:= clock_timestamp();
	duration_time := EXTRACT (EPOCH FROM (end_time - start_time))::int;
	raise notice '>> Loading Duration: % seconds', duration_time;
	raise notice '>>-------------------------------';
	
	raise notice '--- Loading ERP Tables ---';
	
	--erp_cust_az12
	
	start_time:= clock_timestamp();
	raise notice '>> Truncating Table: bronze.erp_cust_az12';
	-- 'TRUNCATE TABLE bronze.erp_cust_az12';
	raise notice'>> Importing into: bronze.erp_cust_az12';
	end_time:= clock_timestamp();
	duration_time := EXTRACT (EPOCH FROM (end_time - start_time))::int;
	raise notice '>> Loading Duration: % seconds', duration_time;
	raise notice '>>-------------------------------';
	
	--erp_loc_a101
	
	start_time:= clock_timestamp();
	raise notice '>> Truncating Table: bronze.erp_loc_a101';
	-- 'TRUNCATE TABLE bronze.erp_loc_a101';
	raise notice'>> Importing into: bronze.erp_loc_a101';
	end_time:= clock_timestamp();
	duration_time := EXTRACT (EPOCH FROM (end_time - start_time))::int;
	raise notice '>> Loading Duration: % seconds', duration_time;
	raise notice '>>-------------------------------';
	
	--erp_px_cat_g1v2
	
	start_time:= clock_timestamp();
	raise notice '>> Truncating Table: bronze.erp_px_cat_g1v2';
	-- 'TRUNCATE TABLE bronze.erp_px_cat_g1v2';
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
