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




	
end;
$$

SELECT bronze.load_bronze_log_only();
