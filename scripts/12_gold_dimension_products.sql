
create view gold.dim_products as

    -- Перейменовуємо стовпці
select 
	row_number() over (order by pn.prd_start_dt, pn.prd_key) as product_key,  -- Створюємо ключ для подальших приєднань таблиць
	pn.prd_id as product_id,
	pn.prd_key as product_number,
	pn.prd_nm as product_name ,
	pn.cat_id as category_id,
	pc.cat as category,
	pc.subcat as subcategory,
	pc.maintenance,
	pn.prd_cost as cost,
	pn.prd_line as product_line,
	pn.prd_start_dt as start_date,
	pn.prd_end_dt as end_date
	
from silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id 
-- where prd_end_dt is null 		-- Фільтруємо продукти, які все ще продаються
