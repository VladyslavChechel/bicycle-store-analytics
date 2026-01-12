/*=============================
-- Customer report
=============================*/

create view gold.report_customers as 

with basic_query as (		-- Вибираємо основні колонки для подальших обчислень
		select 
		s.order_number ,
		s.product_key ,
		s.order_date ,
		s.sales_amount ,
		s.quantity ,
		c.customer_key ,
		c.customer_number ,
		concat(c.first_name, ' ', c.last_name ) as customer_name,
		extract(year from age (c.birthdate) ) as age,
		c.country,
		c.gender
		from gold.fact_sales s
		left join gold.dim_customers c
		on s.customer_key  = c.customer_key 
		where order_date  is not null
), 
customer_aggregation as ( 		-- Основні метрики покупців

	select
			customer_key ,
			customer_number ,
			customer_name,
			age,
			count(distinct order_number) as total_orders,
			SUM(sales_amount) as total_sales,
			SUM(quantity) as total_quantity,
			count(distinct product_key) as total_products,
			max(order_date) as last_order_date, 
			(extract('year' from age(max(order_date),min(order_date) ) ) * 12 
			+ extract('month' from age(max(order_date),min(order_date) ) ))::int as lifespan,
			country,
			gender
	from basic_query
	group by 
			customer_key ,
			customer_number ,
			customer_name,
			age,
			country,
			gender
)

select 							-- Сегментація покупців по категоріям
			customer_key ,
			customer_number ,
			customer_name,
			age,
			country,
			gender,
			case 
				when age < 30 then 'Under 30'
				when age between 30 and 39 then '30-39'
				when age between 40 and 49 then '40-49'
				else 'Above 50'
			end as age_group,
			case 
				when lifespan >= 12 and total_sales > 5000 then 'VIP'
				when lifespan >= 12 and total_sales <= 5000 then 'Regular'
				else 'New'
			end as customer_segment,
			last_order_date,
			extract(month from age(last_order_date) ) as last_order_time,
			total_orders,
			total_sales,
			total_quantity,
			total_products,
			lifespan,
			round((total_sales / total_orders), 0 ) as avg_order_value,
-- Average monthly spend
			case
				when lifespan = 0 then total_sales
				else round(total_sales / lifespan, 0)
			end as avg_monthly_spend
			
from customer_aggregation
