DROP TABLE IF exists silver.erp_loc_a101;

create table silver.erp_loc_a101 (
	cid varchar(50) primary key,
	cntry varchar(50)
);

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
from bronze.erp_loc_a101 
