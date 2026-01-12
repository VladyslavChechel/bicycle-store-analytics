/*
 * ======================================
 *  Analyze sales performance over time
 * ======================================
 */



-- Changes over years
select 
	extract(year from order_date ) as order_year,
	sum(sales_amount) as total_sales,
	count(distinct customer_key) as total_customers,
	sum(quantity) as total_quantity
from gold.fact_sales t 
where order_date is not null
group by order_year
order by order_year


-- Changes over month
select 
	date_trunc('month',  order_date )::date as order_month,
	sum(sales_amount) as total_sales,
	count(distinct customer_key) as total_customers,
	sum(quantity) as total_quantity
from gold.fact_sales t 
where order_date is not null
group by order_month
order by order_month



/*
 * ======================================
 *  Cumulative analysis
 * ======================================
 */


-- Calculate total sales per month 
select 
	date_trunc('month', order_date)::date as order_date,
	sum(sales_amount) as total_sales
from gold.fact_sales s
where order_date is not null
group by date_trunc('month', order_date)::date
order by date_trunc('month', order_date)::date


-- Running total sales (year)
select 
	order_date,
	total_sales,
	sum(total_sales) over (order by order_date) as runnig_total_sales,
	round(avg(avg_price) over (order by order_date),0) as moving_avg_price
from (
	select 
	date_trunc('year', order_date)::date as order_date,
	sum(sales_amount) as total_sales,
	avg(price) as avg_price
	from gold.fact_sales s
	where order_date is not null
	group by date_trunc('year', order_date)::date
) as t




/*
 * ======================================
 * Performance analysis
 * ======================================
 */



-- Analyze the yearly performance of product by comparing avg sales and previous year's sales
with yearly_product_sales as (
		select
			date_trunc('year', s.order_date)::date as order_year,
			p.product_name,
			sum(s.sales_amount) as current_sales  
		from gold.fact_sales s 
		left join gold.dim_products p
		on s.product_key = p.product_key
		where s.order_date  is not null
		group by 
			date_trunc('year', s.order_date)::date,
			p.product_name
)
select 
	order_year,
	product_name,
	current_sales,
	--Year-over_year analysis
	lag(current_sales) over (partition by product_name order by order_year) as py_sales,
	case
		when current_sales - lag(current_sales) over (partition by product_name order by order_year) > 0 then 'Increase'
		when current_sales - lag(current_sales) over (partition by product_name order by order_year) < 0 then 'Decrease'
		else 'No change'
	end as py_change,
	round(avg(current_sales) over (partition by product_name), 0) as avg_sales
from yearly_product_sales
order by 
	product_name,
	order_year
	

	
	
/*
 * ======================================
 * Part-to-Whole analysis
 * ======================================
 */
	
	
-- Category that contribute the most to overall sales
with category_sales as (
		select
		category,
		sum(sales_amount) as total_sales
	from gold.fact_sales s
	left join gold.dim_products p
	on p.product_key = s.product_key
	group by category 
)
select 
	category,
	total_sales,
	SUM(total_sales) over () as overall_sales,
	concat(round( (total_sales / SUM(total_sales) over () ) * 100, 2), '%') as percentage_of_total
from category_sales
order by percentage_of_total desc



-- Sub-Category that contribute the most to overall sales
with category_sales as (
		select
		category,
		subcategory, 
		sum(sales_amount) as total_sales
	from gold.fact_sales s
	left join gold.dim_products p
	on p.product_key = s.product_key
	where category != 'Bikes'
	group by category, 
			 subcategory
)
select 
	category,
	subcategory,
	total_sales,
	SUM(total_sales) over () as overall_sales,
	concat(round( (total_sales / SUM(total_sales) over () ) * 100, 2), '%') as percentage_of_total
from category_sales
order by percentage_of_total desc
