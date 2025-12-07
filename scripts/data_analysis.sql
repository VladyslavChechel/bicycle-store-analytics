
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


-- Досліджуємо дату першого і останнього замовлення

select 
	min(order_date) as first_order,
	max(order_date) as last_order,
	extract (year from  age(max(order_date), min(order_date))) as order_range_years
from gold.fact_sales s


-- Досліджуємо вік покупців

select
	min(birthdate) as youngest_birthday,
	extract(year from age(now(), min(birthdate) )) as oldest_age,
	max(birthdate) as oldest_birthday,
	extract(year from age(now(), max(birthdate) )) as youngest_age
from gold.dim_customers c 


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
	'Total # Orders',
	count(distinct order_number)
from gold.fact_sales

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


-- Customers by countries
select 
	country,
	count(customer_key) as total_customers
from gold.dim_customers 
group by country 
order by total_customers  desc


-- Customers by gender
select 
	gender,
	count(customer_key) as total_customers
from gold.dim_customers 
group by gender  
order by total_customers  desc 


-- Products by category
select 
	category,
	count(product_key) as total_products
from gold.dim_products dp 
group by category 
order by total_products desc


-- AVG cost by category
select 
	category,
	round(avg(cost), 0) as avg_cost
from gold.dim_products dp 
group by category 
order by avg_cost desc


-- Total revenue for each category
select
 	p.category,
 	SUM(s.sales_amount) as total_revenue
 from gold.fact_sales s
 left join gold.dim_products p 
 on p.product_key = s.product_key 
 group by p.category 
 order by total_revenue desc 
 
 
 -- Total revenue by each customer
 select 
 	c.customer_key,
 	c.first_name,
 	c.last_name,
 	sum(s.sales_amount) as total_revenue
 from gold.fact_sales as s
 left join gold.dim_customers as c
 on c.customer_key = s.customer_key
 group by
 	c.customer_key,
 	c.first_name,
 	c.last_name
 order by total_revenue desc

 
 -- Distribution by sold items across countries 
  select 
 	c.country ,
 	sum(s.quantity ) as total_quantity
 from gold.fact_sales as s
 left join gold.dim_customers as c
 on c.customer_key = s.customer_key
 group by
 	c.country
 order by total_quantity desc
 
 
 -- Products which generate the highest revenue
 select 
 	p.product_name,
 	SUM(s.sales_amount) as total_revenue
 from gold.fact_sales s
 left join gold.dim_products p 
 on p.product_key = s.product_key 
 group by p.product_name 
 order by total_revenue desc 
 limit 10
 
 
 -- Worst-performing products 
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
 
 
 
