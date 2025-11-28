-- Перевіряємо NULLs чи Duplicates в Primary key
-- Очікування: no result
select
	cst_id,
	count(*)
from bronze.crm_cust_info 
group by cst_id
having count(*) > 1;


-- Дивимось на діапазон дат
select 
	max(cst_create_date),  --2026-01-27
	min(cst_create_date),   --2025-10-06
	cst_create_date
from bronze.crm_cust_info
group by 3


-- Перевіряємо Duplicates
-- Встановлюємо ранги щоб відокремити Duplicates (залишаємо cst_id за останньою дією)
select 
	*
from (
	select 	
		*,
		rank() over (partition by cst_id order by cst_create_date desc) as rank_last
	from bronze.crm_cust_info cci 
	where cst_id is not null
) as t
where rank_last != 1


-- Перевіряємо колонки на наявність зайвих "Пробілів"
-- (cst_firstname, cst_lastname) - потрібно доопрацювати
-- (cst_key, cst_marital_status, cst_gndr) - все ок
-- Очікування: no result
select 
	cst_gndr
from bronze.crm_cust_info
where cst_gndr != TRIM(cst_gndr)


-- Standardization and Consistency

-- Прибираємо "Пробіли"
-- Змінюємо абреавіатуру на повне значення

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
where rank_last = 1

-- Перевірка стовпців 
select 
	distinct cst_marital_status
from 
	bronze.crm_cust_info

	
-- Завантажуємо очищену таблицю в SILVER
	
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
where rank_last = 1
