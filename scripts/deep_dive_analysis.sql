
-- Cheking why despite equal total revenue, Australia sold  ~35% fewer units?

 select
    country,
    sum(total_sales) as total_sales,
    sum(total_quantity) as total_quantity,
    sum(total_orders) as total_orders,
    round(sum(total_sales) / nullif(sum(total_quantity), 0), 1) as revenue_per_unit,
    round(sum(total_sales) / nullif(sum(total_orders), 0), 1) as avg_order_value,
    round(sum(total_quantity) / nullif(sum(total_orders), 0), 1) as units_per_order
from gold.report_customers
where country in ('Australia', 'United States')
group by country											-- Australia sold  ~35% fewer units, driven by a higher revenue per unit
		


-- Досліджуємо вік покупців
	
	select 
		c.customer_key,
		concat(c.first_name, ' ', c.last_name) as customer_name,
		extract (year from age(c.birthdate) ) as age,             -- найстарший покупець - 109р
		country,
		sum(sales_amount) as total_sales,
		string_agg(distinct p.category, ', ') as categories_purchased
	from gold.dim_customers as c 
	left join gold.fact_sales as s 
	on c.customer_key = s.customer_key 
	left join gold.dim_products as p
	on s.product_key = p.product_key
	where c.birthdate is not null 
 --		and category = 'Bikes'                      -- найстарший покупець який купив Байк- 98р.
	group by 
		c.customer_key,
		customer_name,
		country,
		c.birthdate
	order by age desc
	
	
	-- Analysis of Customers > 80+
	select 
		count(*) as customers
	from 	
		(select 
			extract (year from age(c.birthdate) ) as age,             -- найстарший покупець - 109р
			country
		from gold.dim_customers c 
		where birthdate is not null
		order by age desc
		)
	where age > 79                                                     -- 577 покупців старше 80р;  67 покупці старше 90;  17- старше 100 
	
	
	
	-- Customers segmentation 50+
	with cust_age as 
	(
		select
			count(*) as number_of_cust,
			case 
				when age between 50 and 59 then '50-59'
				when age between 60 and 69 then '60-69'
				when age between 70 and 79 then '70-79'
				when age between 80 and 89 then '80-89'
				when age between 90 and 99 then '90-99'
				when age between 100 and 109 then '100-110'
					
			end as age_group,
			 country
		from 	
			(select 
				extract (year from age(c.birthdate) ) as age,            
				country
			from gold.dim_customers c 
			where birthdate is not null 
			)
		where age > 49
		group by age_group, country
     )   
     select  distinct
     	age_group, 
     	number_of_cust,
     	country
     from cust_age
     order by age_group asc, number_of_cust desc

