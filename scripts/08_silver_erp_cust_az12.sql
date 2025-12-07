DROP TABLE IF exists silver.erp_cust_az12;

create table silver.erp_cust_az12(
	cid varchar(50) primary key,
	bdate date,
	gen varchar(50) 
);

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
	bronze.erp_cust_az12 eca 

	
	


