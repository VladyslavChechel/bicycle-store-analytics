create view gold.over_time_analysis as 
                                                -- Необхідні дані для завантаження в Tableau
select 	
	s.order_date,
	date_trunc('month', s.order_date)::date as order_month,
	date_trunc('year', s.order_date)::date as order_year,
	
	s.sales_amount,
	s.quantity,
	s.price,
	s.customer_key,
	s.product_key,
	
	p.product_name,
	p.category,
	p.subcategory,
	p.cost,
	
	c.customer_number,
	concat(c.first_name, ' ', c.last_name) as customer_name,
	c.country,
	c.gender,
	extract (year from age(c.birthdate) ) as age
from gold.fact_sales s
left join gold.dim_products p
	on s.product_key = p.product_key 
left join gold.dim_customers c
	on s.customer_key = c.customer_key 
where s.order_date is not null
	

