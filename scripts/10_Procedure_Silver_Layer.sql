/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
===============================================================================
*/

create or replace function silver.load_silver ()
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
	raise notice 'Loading Silver Layer';
	raise notice '=================================';

	raise notice '--- Loading CRM Tables ---';


	-- Loading silver.crm_cust_info

	start_time:= clock_timestamp();
	raise notice '>> Truncating Table: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	raise notice'>> Inserting data into: silver.crm_cust_info';
	
		insert into silver.crm_cust_info (
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
			trim(cst_firstname) as cst_firstname ,
			trim(cst_lastname) as cst_lastname,
			case 
				when upper(cst_marital_status) = 'S' then 'Single'
				when upper(cst_marital_status) = 'M' then 'Married'
				else 'Unknown'
			end as cst_marital_status,
			case 
				when upper(cst_gndr) = 'M' then 'Male'
				when upper(cst_gndr) = 'F' then 'Female'
				else 'Unknown'
			end as cst_gndr,
			cst_create_date
		from (
			select 	
				*,
				rank() over (partition by cst_id order by cst_create_date desc) as rank_last
			from bronze.crm_cust_info cci 
			where cst_id is not null
		) as t
		where rank_last = 1;
	end_time:= clock_timestamp();
	duration_time := EXTRACT (EPOCH FROM (end_time - start_time))::int;
	raise notice '>> Loading Duration: % seconds', duration_time;
	raise notice '>>-------------------------------';
	

	-- Loading silver.crm_prd_info
	
	start_time:= clock_timestamp();
	raise notice '>> Truncating Table: silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	raise notice'>> Importing into: silver.crm_prd_info';

		INSERT INTO silver.crm_prd_info (
	    prd_id,
	    cat_id,
	    prd_key,
	    prd_nm,
	    prd_cost,
	    prd_line,
	    prd_start_dt,
	    prd_end_dt
		)
		select 
		prd_id,
		replace (substring(prd_key, 1, 5),'-', '_') as cat_id, 	-- Приводимо стовпець prd_key до формату, який відповідає категорії товарів id в таблиці erp_px_cat_g1v2 для подальшого приєднання таблиць
		substring(prd_key, 7, length(prd_key)) as prd_key, 		-- Приводимо стовпець prd_key до формату, який відповідає стовпцю sls_prd_key в таблиці crm_sales_details для подальшого приєднання таблиць
		prd_nm,
		COALESCE(prd_cost, 0) as prd_cost,						-- Замінюємо NULL на "0"
		case UPPER(TRIM(prd_line))								-- Замінюємо абревіатуру на повну назву в prd_line
			when 'M' then 'Mountain'
			when 'R' then 'Road'
			when 'S' then 'Others'
			when 'T' then 'Touring'
			else 'Unknown'
		end as prd_line,
		prd_start_dt,
		lead(prd_start_dt) over (partition by prd_key order by prd_start_dt) -1 as prd_end_dt   -- Змаміна дат для стовпця prd_end_dt (мінус одинь день від наступної транзакції для окремого prd_key ) 
		from bronze.crm_prd_info;

		end_time:= clock_timestamp();
		duration_time := EXTRACT (EPOCH FROM (end_time - start_time))::int;
		raise notice '>> Loading Duration: % seconds', duration_time;
		raise notice '>>-------------------------------';

	-- Loading silver.crm_sales_details

	start_time:= clock_timestamp();
	raise notice '>> Truncating Table: silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;
	raise notice'>> Importing into: silver.crm_sales_details';

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
	
	WITH step1 AS (
	    select
	    	sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
	        sls_quantity,
	        sls_sales  AS old_sls_sales,
	        sls_price  AS old_sls_price,
	        
	        CASE
	            WHEN sls_sales IS NULL
	              OR sls_sales <= 0
	              OR sls_sales != sls_quantity * ABS(sls_price)
	            THEN sls_quantity * ABS(sls_price)
	            ELSE sls_sales
	        END AS new_sls_sales,
	        
	        ABS(sls_price) AS tmp_price
	    FROM bronze.crm_sales_details
	  
	)
	SELECT 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
	    new_sls_sales AS sls_sales,
	    sls_quantity,
	    
	    CASE
	        WHEN tmp_price <= 0
	        THEN new_sls_sales / NULLIF(sls_quantity, 0)
	        ELSE tmp_price
	    END AS sls_price
	    
	FROM step1;

	end_time:= clock_timestamp();
		duration_time := EXTRACT (EPOCH FROM (end_time - start_time))::int;
		raise notice '>> Loading Duration: % seconds', duration_time;
		raise notice '>>-------------------------------';
	

	raise notice '--- Loading ERP Tables ---';


	-- Loading silver.erp_cust_az12

	start_time:= clock_timestamp();
	raise notice '>> Truncating Table: silver.erp_cust_az12';
	TRUNCATE TABLE silver.erp_cust_az12;
	raise notice'>> Importing into: silver.erp_cust_az12';

	insert into silver.erp_cust_az12 (
	cid,
	bdate,
	gen
	)
		-- Перевіряємо коректність cid та визначаємо подальшу можливість обʼєднання з іншими таблицями за ключем
		-- Перевірка валідності дати народження
		-- Перевірка унікальних значень в gen
	select 
		case 
			when cid like 'NAS%' then substring(cid, 4, length(cid))  -- Прибираємо непотрібний префікс 
			else cid
		end as cid,
		case 
			when bdate > now() then null 		-- Помічаємо, що певні ДН більші ніж поточна дата, замінюємо такі дати на null
			else bdate
		end as bdate,
		case 
			when upper (gen) like 'F%' then 'Female'  -- Приводимо стовпець до єдиного стандарту значень
			when upper (gen) like 'M%' then 'Male'
			else 'Unknown'
		end as gen
	from 
		bronze.erp_cust_az12; 
	
	end_time:= clock_timestamp();
	duration_time := EXTRACT (EPOCH FROM (end_time - start_time))::int;
	raise notice '>> Loading Duration: % seconds', duration_time;
	raise notice '>>-------------------------------';

	-- Loading silver.erp_loc_a101

	start_time:= clock_timestamp();
	raise notice '>> Truncating Table: silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;
	raise notice'>> Importing into: silver.erp_loc_a101';

	insert into silver.erp_loc_a101(
	cid,
	cntry
	)
	
	-- Перевіряємо cid та можливість зʼєднання за ключем з таблицею crm_cust_info
	select 
		replace (cid, '-', '') as cid,  -- Прибираємо зайві символи, до відповідності до cst_key в табл crm_cust_info
		case 
			when trim(cntry) = 'DE' then 'Germany'
			when trim(cntry) in ('US', 'USA') then 'United States'
			when trim(cntry) = '' or cntry is null then 'Unknown'
			else TRIM(cntry)
		end as cntry					-- Привели всі абревіатури до єдиного стандарту
	from bronze.erp_loc_a101 ;

	end_time:= clock_timestamp();
	duration_time := EXTRACT (EPOCH FROM (end_time - start_time))::int;
	raise notice '>> Loading Duration: % seconds', duration_time;
	raise notice '>>-------------------------------';

	-- Loading silver.erp_px_cat_g1v2

	start_time:= clock_timestamp();
	raise notice '>> Truncating Table: silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	raise notice'>> Importing into: silver.erp_px_cat_g1v2';

	insert into silver.erp_px_cat_g1v2 (
	  id,		
	  cat,
	  subcat,
	  maintenance
	)
	
	select 
		id,		-- Стовпець відповідає cat_id в таблиці crm_prd_info
		cat,
		subcat,
		maintenance
	from bronze.erp_px_cat_g1v2;

	end_time:= clock_timestamp();
	duration_time := EXTRACT (EPOCH FROM (end_time - start_time))::int;
	raise notice '>> Loading Duration: % seconds', duration_time;
	raise notice '>>-------------------------------';


	batch_end_time:= clock_timestamp();
	raise notice '==================================';
	raise notice 'Loading Silver Layer is Completed';
	raise notice '==================================';
	
	exception 
		when others then 
		--output error
		raise notice '==================================';
		raise notice 'Error Occurred During Loading Silver Layer ';
		raise notice 'SQLERRM: %', SQLERRM;
		raise notice 'SQLSTATE: %', SQLSTATE;
		raise notice '==================================';

end;
$$
