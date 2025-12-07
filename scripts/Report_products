/*==========================
-- Product Report
==========================*/
create view gold.report_products as
with basic_query as (	-- Основні колонки для аналізу
	select 
		p.product_key ,
		p.product_name ,
		p.category ,
		p.subcategory ,
		p.product_line,
		p.cost ,
		s.sales_amount ,
		s.quantity ,
		s.price ,
		s.customer_key ,
		s.order_date ,
		s.order_number		
	from gold.dim_products p 
	left join GOLD.fact_sales s
	on p.product_key = s.product_key
	where s.order_date is not null			-- Працюємо з валідними датами
),
product_aggregation as (
	select 					-- Основні продуктові метрики
		product_key ,
		product_name ,
		category ,
		subcategory ,
		product_line,
		cost ,
		price,
		count(distinct order_number) as total_orders,
		sum(sales_amount ) as total_sales,
		sum(quantity ) as total_quantity,
		count(distinct customer_key) as total_customers,
		(extract('year' from age(max(order_date),min(order_date) ) ) * 12 
		+ extract('month' from age(max(order_date),min(order_date) ) ))::int as lifespan,
		max(order_date) as last_order_date		
	from basic_query
	group by
		product_key,
		product_name ,
		category ,
		subcategory ,
		product_line,
		cost,
		price
)
select					-- Сегментація продуктів
	product_name ,
	category ,
	subcategory ,
	product_line,
	cost ,
	price,
	total_orders,
	total_sales,
	case 
		when total_sales > 500000 then 'High-Performer'
		when total_sales >= 100000 then 'Mid-Range'
		else 'Low-Performer'
	end as product_segment,
	total_quantity,
	total_customers,
	lifespan,
	last_order_date,
	extract(month from age(last_order_date) ) as last_order_time,
	case 				-- Average Order Revenue
		when total_orders = 0 then total_orders
		else round(total_sales / total_orders, 0)
	end as avg_order_revenue,
	case 				-- Average Monthly Revenue
		when  lifespan = 0 then lifespan
		else round(total_sales / lifespan, 0)
	end as avg_monthly_revenue
from product_aggregation
