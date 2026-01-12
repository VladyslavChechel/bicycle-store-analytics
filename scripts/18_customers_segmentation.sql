/*
======================
Customers Segmentation
======================
*/

-- Segment products into cost ranges
with product_segment as (
		select 
			product_key ,
			product_name ,
			cost,
			case 
				when cost < 100 then 'Below 100'
				when cost between 100 and 500 then '100-500'
				when cost between 500 and 1000 then '500-1000'
				else 'Above 1000'
			end as cost_range	
		from gold.dim_products 
)
select 
	cost_range,
	count(product_key ) as total_products
from product_segment
group by cost_range 
order by total_products desc



-- Customers with at least 12 month of history and spend more than 5000
-- Customers with at least 12 month of history and spend less than 5000
-- Customers less than 12 month
-- Total # of customers of each group
with customer_spending as(
		select 
			c.customer_key,
			sum(s.sales_amount) as total_spending,
			min(s.order_date) as first_order,
			max(s.order_date) as last_order,
			(extract('year' from age(max(s.order_date),min(s.order_date) ) ) * 12 
			+ extract('month' from age(max(s.order_date),min(s.order_date) ) ))::int as lifespan
		from gold.fact_sales s
		left join gold.dim_customers c
		on s.customer_key = c.customer_key 
		group by c.customer_key
), 
segment as(
		select 
			customer_key,
			case 
				when lifespan >= 12 and total_spending > 5000 then 'VIP'
				when lifespan >= 12 and total_spending <= 5000 then 'Regular'
				else 'New'
			end as customer_segment	
		from customer_spending
) 
select
	customer_segment,
	count(cs.customer_key) as total_customers
from customer_spending as cs
join segment as s
on cs.customer_key = s.customer_key
group by customer_segment
order by total_customers
