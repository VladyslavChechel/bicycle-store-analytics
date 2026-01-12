/*
 =============
 Dimensions Exploration
 ============= 
 */


-- Досліджуємо, з яких країн маємо дані по клієнтам

select distinct
	country
from gold.dim_customers c 


-- Досліджуємо категорії товарів

select distinct 
	category,
	subcategory,
	product_name
from gold.dim_products p
order by 1,2,3




/*
 =============
 Date Range Exploration 
 =============
 */

-- Досліджуємо дату першого і останнього замовлення

select 
	min(order_date) as first_order,
	max(order_date) as last_order,
	(extract (year from  age(max(order_date), min(order_date))))::int as order_range_years
from gold.fact_sales s


-- Досліджуємо вік покупців

select
	min(birthdate) as youngest_birthday,
	extract(year from age(now(), min(birthdate) )) as oldest_age,
	max(birthdate) as oldest_birthday,
	extract(year from age(now(), max(birthdate) )) as youngest_age
from gold.dim_customers c 



/*
 =============
Measures Exploration (Key Metrics)
==============
 */


-- Виводимо ключові бізнес-метрики

select 
	'Total Sales' as measure_name,
	SUM(sales_amount) as measure_value
from gold.fact_sales 
	
union all

select 
	'Total Quantity',
	SUM(quantity)
from gold.fact_sales 

union all

select
	'Average Price',
	round(avg(price), 0)
from gold.fact_sales

union all

select
	'Average Sales',
	round(AVG(sales_amount), 0)
from gold.fact_sales

union all

select
	'Total # Orders',
	count(distinct order_number)
from gold.fact_sales

union all

select 
	'Total # category',
	count(distinct category )
from gold.dim_products

union all

select 
	'Total # subcategory',
	count(distinct subcategory )
from gold.dim_products

union all
	
select 
	'Total # Products',
	count(product_name)
from gold.dim_products

union all

select 
	'Total # Customers',
	count(customer_key)
from gold.dim_customers 




/*
 =============
Magnitude Analysis
==============
 */

-- Покупці за країнами
select 
	country,
	count(customer_key) as total_customers
from gold.dim_customers 
group by country 
order by total_customers  desc


-- Продажі за країнами
select 
	dc.country,
	sum(t.sales_amount) as total_sales
from gold.dim_customers dc
left join gold.fact_sales t 
on dc.customer_key = t.customer_key 
group by country
order by total_sales desc


-- Стать покупців
select 
	gender,
	count(customer_key) as total_customers
from gold.dim_customers 
group by gender  
order by total_customers  desc 


-- Продукти по категоріям
select 
	category,
	count(product_key) as total_products
from gold.dim_products dp 
group by category 
order by total_products desc


-- AVG вартість товарів по категоріям
select 
	category,
	round(avg(cost), 0) as avg_cost
from gold.dim_products dp 
group by category 
order by avg_cost desc


-- Дохід по категоріям
select
 	p.category,
 	SUM(s.sales_amount) as total_revenue
 from gold.fact_sales s
 left join gold.dim_products p 
 on p.product_key = s.product_key 
 group by p.category 
 order by total_revenue desc 
 
 
 -- Дохід по клієнтам
 select 
 	c.customer_key,
 	c.first_name,
 	c.last_name,
 	sum(s.sales_amount) as total_revenue,
 	c.country 
 from gold.fact_sales as s
 left join gold.dim_customers as c
 on c.customer_key = s.customer_key
 group by
 	c.customer_key,
 	c.first_name,
 	c.last_name,
 	c.country 
 order by total_revenue desc

 
 -- Розподіл за проданими товарами по країнах 
  select 
 	c.country ,
 	sum(s.quantity ) as total_quantity
 from gold.fact_sales as s
 left join gold.dim_customers as c
 on c.customer_key = s.customer_key
 group by
 	c.country
 order by total_quantity desc
 
 
 
 
 /*
==============
Ranking Analysis
==============
 */

 -- Продукти, які генерують найвищий дохід
 select 
 	p.product_name,
 	SUM(s.sales_amount) as total_revenue
 from gold.fact_sales s
 left join gold.dim_products p 
 on p.product_key = s.product_key 
 group by p.product_name 
 order by total_revenue desc 
 limit 10
 
 
 -- Продукти з найгіршими показниками
 select *
 from (
		 select 
		 	p.product_name,
		 	SUM(s.sales_amount) as total_revenue,
		 	row_number () over (order by SUM(s.sales_amount) desc ) as rank_products
		 from gold.fact_sales s
		 left join gold.dim_products p 
		 on p.product_key = s.product_key 
		 group by p.product_name
 )
 where rank_products <= 10
 
 
 
