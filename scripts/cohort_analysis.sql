-- Cohort Analysis (Year)

WITH first_purchase AS (
    SELECT
        customer_key ,
        DATE_TRUNC('month', MIN(order_date)) AS cohort_month,
        DATE_PART('year', MIN(order_date)) AS cohort_year
    FROM gold.fact_sales
    GROUP BY customer_key
),

customer_activity AS (
    SELECT DISTINCT
        f.customer_key,
        fp.cohort_year,
        DATE_PART(
            'month',
            AGE(
                DATE_TRUNC('month', f.order_date),
                fp.cohort_month
            )
        ) AS month_number
    FROM gold.fact_sales f
    JOIN first_purchase fp
        ON f.customer_key = fp.customer_key
    WHERE DATE_PART(
        'month',
        AGE(
            DATE_TRUNC('month', f.order_date),
            fp.cohort_month
        )
    ) BETWEEN 0 AND 12
)

SELECT
    cohort_year,
    month_number,
    COUNT(DISTINCT customer_key) AS active_customers
FROM customer_activity
GROUP BY cohort_year, month_number
ORDER BY cohort_year, month_number;





-- Cohort Analysis (% of total)


WITH first_purchase AS (
    SELECT
        customer_key,
        DATE_TRUNC('month', MIN(order_date)) AS cohort_month
    FROM gold.fact_sales
    GROUP BY customer_key
),

customer_activity AS (
    SELECT DISTINCT
        f.customer_key,
        DATE_PART(
            'month',
            AGE(
                DATE_TRUNC('month', f.order_date),
                fp.cohort_month
            )
        ) AS month_number
    FROM gold.fact_sales f
    JOIN first_purchase fp
        ON f.customer_key = fp.customer_key
    WHERE DATE_PART(
        'month',
        AGE(
            DATE_TRUNC('month', f.order_date),
            fp.cohort_month
        )
    ) BETWEEN 0 AND 12
),

cohort_size AS (
    SELECT COUNT(DISTINCT customer_key) AS cohort_size
    FROM first_purchase
)

SELECT
    ca.month_number,
    COUNT(DISTINCT ca.customer_key) AS active_customers,
    cs.cohort_size,
    ROUND(
        COUNT(DISTINCT ca.customer_key)::numeric
        / cs.cohort_size
        * 100,
        2
    ) AS retention_pct
FROM customer_activity ca
CROSS JOIN cohort_size cs
GROUP BY ca.month_number, cs.cohort_size
ORDER BY ca.month_number;




-- К-сть клієнтів, які зробили лише 1 замовлення

with orders_per_customer as (
    select
        c.customer_number as cust_numb,
        count(distinct t.order_number) as total_orders
    from gold.fact_sales t
    left join gold.dim_customers c
        on t.customer_key = c.customer_key
    group by c.customer_number
)

select
    count(*) as customers_with_one_order 		-- ~63% покупців, зробили лише 1 замовлення
from orders_per_customer
where total_orders = 1;



	


select
    c.customer_number as cust_numb,
    count(distinct t.order_number) as total_orders,
    min(t.order_date) as first_order_date,
    max(t.order_date) as last_order_date
from gold.fact_sales t
left join gold.dim_customers c
    on t.customer_key = c.customer_key
group by c.customer_number;




WITH first_purchase AS (
    SELECT
        customer_key,
        DATE_TRUNC('quarter', MIN(order_date)) AS first_purchase_q
    FROM gold.fact_sales 
    GROUP BY customer_key
),
orders_with_elapsed AS (
    SELECT
        o.customer_key,
        fp.first_purchase_q,
        DATE_TRUNC('quarter', o.order_date) AS order_q,
        (
          (EXTRACT(YEAR FROM DATE_TRUNC('quarter', o.order_date)) 
           - EXTRACT(YEAR FROM fp.first_purchase_q)) * 4
          +
          (EXTRACT(QUARTER FROM DATE_TRUNC('quarter', o.order_date)) 
           - EXTRACT(QUARTER FROM fp.first_purchase_q))
        ) AS elapsed_quarters
    FROM gold.fact_sales o
    JOIN first_purchase fp
        ON o.customer_key = fp.customer_key
    WHERE fp.first_purchase_q BETWEEN '2011-01-01' AND '2011-12-31'
)
SELECT *
FROM orders_with_elapsed
ORDER BY first_purchase_q, customer_key, elapsed_quarters;






SELECT
    s.customer_key,
    COUNT(DISTINCT s.order_number) AS bike_orders
FROM gold.fact_sales s
JOIN gold.dim_products p
    ON s.product_key = p.product_key
WHERE p.category = 'Bikes'
GROUP BY s.customer_key
HAVING COUNT(DISTINCT s.order_number) > 1
ORDER BY bike_orders DESC;




WITH first_purchase AS (
    SELECT
        c.customer_key,
        c.country,
        DATE_TRUNC('quarter', MIN(s.order_date)) AS first_purchase_q
    FROM gold.fact_sales s
    JOIN gold.dim_customers c
        ON s.customer_key = c.customer_key
    GROUP BY
        c.customer_key,
        c.country
),
orders_with_elapsed AS (
    SELECT
        s.customer_key,
        fp.country,
        fp.first_purchase_q,
        DATE_TRUNC('quarter', s.order_date) AS order_q,
        (
          (EXTRACT(YEAR FROM DATE_TRUNC('quarter', s.order_date))
           - EXTRACT(YEAR FROM fp.first_purchase_q)) * 4
          +
          (EXTRACT(QUARTER FROM DATE_TRUNC('quarter', s.order_date))
           - EXTRACT(QUARTER FROM fp.first_purchase_q))
        ) AS elapsed_quarters
    FROM gold.fact_sales s
    JOIN first_purchase fp
        ON s.customer_key = fp.customer_key
)
SELECT
    country,
    first_purchase_q,
    elapsed_quarters,
    COUNT(DISTINCT customer_key) AS active_customers
FROM orders_with_elapsed
WHERE elapsed_quarters >= 0
GROUP BY
    country,
    first_purchase_q,
    elapsed_quarters
ORDER BY
    country,
    first_purchase_q,
    elapsed_quarters;


