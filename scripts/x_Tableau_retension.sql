-- RETENTION 



-- Перевірка візуаліцації в Tableau

-- Знайти ПЕРШУ покупку кожного клієнта (у кварталах)
-- чи є клієнти з first_purchase у 2011 Q1–Q4?    Відповідь- ТАК!

WITH first_purchase AS (
    SELECT
        customer_key,
        DATE_TRUNC('quarter', MIN(order_date)) AS first_purchase_q
    FROM gold.fact_sales t 
    GROUP BY customer_key
)
SELECT *
FROM first_purchase
WHERE first_purchase_q BETWEEN '2011-01-01' AND '2011-12-31'
ORDER BY first_purchase_q, customer_key;



-- Розмір кожної когорти 2011 року
-- Розмір когорт співпадає з Tableau

WITH first_purchase AS (
    SELECT
        customer_key,
        DATE_TRUNC('quarter', MIN(order_date)) AS first_purchase_q
    FROM gold.fact_sales 
    GROUP BY customer_key
)
SELECT
    first_purchase_q,
    COUNT(DISTINCT customer_key) AS customers_in_cohort
FROM first_purchase
WHERE first_purchase_q BETWEEN '2011-01-01' AND '2011-12-31'
GROUP BY first_purchase_q
ORDER BY first_purchase_q;



-- ВСІ покупки клієнтів із цих когорт + elapsed quarters


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
where elapsed_quarters = 4
ORDER BY first_purchase_q, customer_key, elapsed_quarters;



