
-- Аналіз даних покупців шляхом приєднання таблиць

select 			-- Перевірка на дублікати
	cst_id,
	count(*)
from (

	select 
		ci.cst_id,
		ci.cst_key,
		ci.cst_firstname,
		ci.cst_lastname,
		ci.cst_marital_status,
		ci.cst_gndr,
		ci.cst_create_date,
		ca.bdate,
		ca.gen,
		la.cntry 
	from silver.crm_cust_info ci 
	left join silver.erp_cust_az12 ca 
	on 		ci.cst_key = ca.cid 
	left join silver.erp_loc_a101 la
	on 		ci.cst_key = la.cid
	)	 as t
group by 
	cst_id
having count(*) > 1


-- 	Перевірка ствопців, з інф по статті, різних таблиць на співпадіння даних
	select 	
		ci.cst_gndr,	-- ci.cst_gndr - Основна таблиця для подальшого орієнтиру	
		ca.gen,
		case 			-- Помічаємо проблему та приймаємо рішення щодо виправлення
			when ci.cst_gndr != 'Unknown' then ci.cst_gndr
			else coalesce(ca.gen, 'Unknown' )
		end as new_gen
		
	from silver.crm_cust_info ci 
	left join silver.erp_cust_az12 ca 
	on 		ci.cst_key = ca.cid 
	left join silver.erp_loc_a101 la
	on 		ci.cst_key = la.cid


-- Перейменування стовпців для більш зрозумілого контексту
	select
		row_number() over (order by cst_id) as customer_key,  -- Створення порядкового номера клієнта, для подальшого використання в якості ключа зʼєднання таблиць
		ci.cst_id as customer_id,
		ci.cst_key as customer_number,
		ci.cst_firstname as first_name,
		ci.cst_lastname as last_name,
		la.cntry as country,
		ci.cst_marital_status as marital_status,
		case 			-- Помічаємо проблему та приймаємо рішення щодо виправлення
			when ci.cst_gndr != 'Unknown' then ci.cst_gndr
			else coalesce(ca.gen, 'Unknown' )
		end as gender,
		ca.bdate as birthdate,
		ci.cst_create_date as create_date
		
	from silver.crm_cust_info ci 
	left join silver.erp_cust_az12 ca 
	on 		ci.cst_key = ca.cid 
	left join silver.erp_loc_a101 la
	on 		ci.cst_key = la.cid
	
	
	-- Створення schema gold	
	-- Створення VIEW 
	
	create schema if not exists gold;
	
	create view gold.dim_customers as
		select
		row_number() over (order by cst_id) as customer_key,  -- Створення порядкового номера клієнта, для подальшого використання в якості ключа зʼєднання таблиць
		ci.cst_id as customer_id,
		ci.cst_key as customer_number,
		ci.cst_firstname as first_name,
		ci.cst_lastname as last_name,
		la.cntry as country,
		ci.cst_marital_status as marital_status,
		case 			-- Помічаємо проблему та приймаємо рішення щодо виправлення
			when ci.cst_gndr != 'Unknown' then ci.cst_gndr
			else coalesce(ca.gen, 'Unknown' )
		end as gender,
		ca.bdate as birthdate,
		ci.cst_create_date as create_date
		
	from silver.crm_cust_info ci 
	left join silver.erp_cust_az12 ca 
	on 		ci.cst_key = ca.cid 
	left join silver.erp_loc_a101 la
	on 		ci.cst_key = la.cid
	
	
