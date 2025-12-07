drop table if exists silver.erp_px_cat_g1v2;

create table silver.erp_px_cat_g1v2 (
	  id varchar(50) primary key,
	  cat varchar(50) not null,
	  subcat varchar(50) not null,
	  maintenance varchar(50) not null
);

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
from bronze.erp_px_cat_g1v2 epcgv 


-- Перевірка на зайві пробіли
select 
	*
from bronze.erp_px_cat_g1v2
where cat != TRIM(cat) or subcat != TRIM(subcat) or maintenance != TRIM(maintenance)


-- Перевірка унікальних даних в колонках cat, subcat, maintenance
	
select distinct
	maintenance
from bronze.erp_px_cat_g1v2
