
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
		


