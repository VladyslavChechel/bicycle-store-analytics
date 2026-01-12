DROP TABLE IF EXISTS silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info (
    prd_id       INT,
    cat_id       VARCHAR(50),
    prd_key      VARCHAR(50),
    prd_nm       VARCHAR(50),
    prd_cost     INT,
    prd_line     VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt   DATE
);

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
	replace (substring(prd_key, 1, 5),'-', '_') as cat_id, 	-- Перетворюємо стовпець prd_key у формат, який відповідає категорії товарів id в таблиці erp_px_cat_g1v2 для подальшого приєднання таблиць
	substring(prd_key, 7, length(prd_key)) as prd_key, 		-- Перетворюємо стовпець prd_key у формат, який відповідає стовпцю sls_prd_key в таблиці crm_sales_details для подальшого приєднання таблиць
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
from bronze.crm_prd_info



-- Перевірка на дублікати та NULLs
select
	prd_id,
	count(*)
from silver.crm_prd_info 
group by prd_id
having count(*) > 1 or prd_id is null;

-- Перевірка на зайві "Пробіли"
select 
	prd_nm
from silver.crm_prd_info
where prd_nm != TRIM(prd_nm)

-- Перевірка стовпців 
select 
	distinct prd_line 
from 
	silver.crm_prd_info
	
-- Перевірка NULL та відʼємні значення
select 
	prd_cost
from silver.crm_prd_info
where prd_cost < 0 or prd_cost is null


-- Перевірка валідності стовпців з датами
select
	*
from 
	silver.crm_prd_info
where prd_end_dt  < prd_start_dt 

-- Тестуємо змаміну дат для стовпця prd_end_dt (мінус одинь день від наступної транзакції для окремого prd_key )
select
	prd_id,
	prd_key,
	prd_nm, 
	prd_start_dt,
	prd_end_dt,
	lead(prd_start_dt) over (partition by prd_key order by prd_start_dt) -1 as prd_end_dt
from 
	silver.crm_prd_info
where prd_key in ('AC-HE-HL-U509', 'AC-HE-HL-U509-R')
 
