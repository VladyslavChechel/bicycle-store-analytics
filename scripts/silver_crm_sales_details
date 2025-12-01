-- Перевірка валідності дат
select
	*
from 
	bronze.crm_sales_details
where  sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt

	
-- Перевірка даних в стовпцях sls_sales, sls_quantity, sls_price
-- >> sls_sales = sls_quantity * sls_price
-- >> Значення не повинні бути NULL, ZERO чи відʼємними 
WITH step1 AS (
    SELECT
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
    WHERE 
        sls_sales IS NULL
        OR sls_sales <= 0
        OR sls_price IS NULL
        OR sls_price <= 0
        OR sls_sales != sls_quantity * sls_price
)
SELECT DISTINCT
    sls_quantity,
    new_sls_sales AS sls_sales,
    
    CASE
        WHEN tmp_price <= 0
        THEN new_sls_sales / NULLIF(sls_quantity, 0)
        ELSE tmp_price
    END AS sls_price
    
FROM step1;

======================================================

DROP TABLE IF EXISTS silver.crm_sales_details;

create table silver.crm_sales_details (
	sls_ord_num varchar(50) not null ,
	sls_prd_key varchar(50) not null,
	sls_cust_id int not null,
	sls_order_dt date,
	sls_ship_dt date,
	sls_due_dt date,
	sls_sales numeric(10, 2) not null,
	sls_quantity int not null,
	sls_price numeric(10, 2) not null
);


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

